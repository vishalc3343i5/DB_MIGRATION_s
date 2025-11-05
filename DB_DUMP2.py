"""
Streamlit app: MongoDB UAT -> Target dump & restore without dropping or deleting

This version automatically maps source DB to target DB if names differ,
and removes all code related to dropping or deleting target databases or temporary files.
"""

import streamlit as st
import subprocess
import tempfile
import os
import shlex
import time
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError, OperationFailure

st.set_page_config(page_title="Mongo copy (UAT -> Target)", page_icon="📦")

def verify_connection(uri: str, timeout_ms: int = 5000):
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=timeout_ms)
        dbs = client.list_database_names()
        sample = ', '.join(dbs[:5]) if dbs else '(no dbs)'
        return True, f"Connected. Databases: {sample}"
    except ServerSelectionTimeoutError as e:
        return False, f"Timeout / cannot reach server: {e}"
    except OperationFailure as e:
        return False, f"Auth failed or operation not permitted: {e}"
    except Exception as e:
        return False, f"Other error: {e}"

def run_cmd(cmd: list, stream_log_callback=None):
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    except FileNotFoundError as e:
        raise RuntimeError(f"Command not found: {cmd[0]}. Ensure MongoDB tools path is correct.")

    output_lines = []
    while True:
        line = proc.stdout.readline()
        if line == '' and proc.poll() is not None:
            break
        if line:
            output_lines.append(line)
            if stream_log_callback:
                stream_log_callback(line)
    retcode = proc.poll()
    if retcode != 0:
        raise RuntimeError(f"Command failed (exit {retcode}). Last lines:\n" + ''.join(output_lines[-20:]))
    return ''.join(output_lines)

st.title("MongoDB UAT → Target: dump & restore")

st.caption("Use this tool to copy a database from a source (UAT) MongoDB instance to a target instance.")

st.subheader("Connections")
col1, col2 = st.columns(2)
with col1:
    source_uri = st.text_input("Source MongoDB URI (UAT)", placeholder="mongodb+srv://user:pass@cluster/test")
with col2:
    target_uri = st.text_input("Target MongoDB URI", placeholder="mongodb://user:pass@host:port")

st.subheader("MongoDB Tools Path")
MONGO_BIN_PATH = st.text_input(
    "Path to MongoDB bin folder (e.g., C:\\Program Files\\MongoDB\\Server\\5.0\\bin)",
    value="",
    help="Path where mongodump.exe and mongorestore.exe are located. Leave blank if tools are on PATH."
)

st.subheader("Dump options")
selected_db = st.text_input("Source database to copy (leave empty to copy ALL databases)")
target_db_name = st.text_input("Target database name (leave empty to keep same as source)")

if st.button("Verify connections"):
    if not source_uri or not target_uri:
        st.error("Please provide both source and target URIs before verifying.")
    else:
        with st.spinner("Verifying source..."):
            ok_src, msg_src = verify_connection(source_uri)
        with st.spinner("Verifying target..."):
            ok_tgt, msg_tgt = verify_connection(target_uri)
        st.write("Source:", msg_src)
        st.write("Target:", msg_tgt)

run_btn = st.button("Run dump → restore")
log_area = st.empty()

if run_btn:
    if not source_uri or not target_uri:
        st.error("Both source and target URIs are required.")
    else:
        ok_src, msg_src = verify_connection(source_uri)
        ok_tgt, msg_tgt = verify_connection(target_uri)
        if not ok_src or not ok_tgt:
            st.error(f"Connection issue.\nSource: {msg_src}\nTarget: {msg_tgt}")
        else:
            tmpdir = tempfile.mkdtemp(prefix="mongo-copy-")
            archive_path = os.path.join(tmpdir, f"dump-{int(time.time())}.archive.gz")

            mongodump_path = os.path.join(MONGO_BIN_PATH, "mongodump.exe") if MONGO_BIN_PATH else "mongodump"
            mongorestore_path = os.path.join(MONGO_BIN_PATH, "mongorestore.exe") if MONGO_BIN_PATH else "mongorestore"

            dump_cmd = [
                mongodump_path,
                f"--uri={source_uri}",
                f"--archive={archive_path}",
                "--gzip",
            ]
            if selected_db:
                dump_cmd.append(f"--db={selected_db}")

            restore_cmd = [
                mongorestore_path,
                f"--uri={target_uri}",
                f"--archive={archive_path}",
                "--gzip",
            ]

            # Automatically map source to target DB if names differ
            if selected_db and target_db_name and target_db_name != selected_db:
                restore_cmd.append(f"--nsFrom={selected_db}.*")
                restore_cmd.append(f"--nsTo={target_db_name}.*")

            def make_logger(area):
                buffer = []
                def log(line: str):
                    buffer.append(line)
                    area.text_area("Logs", value=''.join(buffer[-2000:]), height=300)
                return log

            logger = make_logger(log_area)

            try:
                with st.spinner("Running mongodump..."):
                    logger(f"Running: {' '.join(shlex.quote(p) for p in dump_cmd)}\n")
                    run_cmd(dump_cmd, stream_log_callback=logger)
                    logger("mongodump finished successfully.\n")

                with st.spinner("Running mongorestore..."):
                    logger(f"Running: {' '.join(shlex.quote(p) for p in restore_cmd)}\n")
                    run_cmd(restore_cmd, stream_log_callback=logger)
                    logger("mongorestore finished successfully.\n")

                st.success("Copy completed successfully without dropping or deleting.")

            except Exception as e:
                st.error(f"Error during dump/restore: {e}")
                logger(f"ERROR: {e}\n")
                st.warning("Ensure the MongoDB tools path is set correctly or tools are in PATH.")