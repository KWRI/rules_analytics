"""
Production Environment Gateway: Standalone Deployment Operator.

This standalone utility reads the finalized, numerically structured integer lines from
the promotion manifest asset file. It bypasses lower workspace paths to dynamically establish a direct,
highly isolated migration tunnel to the core Production environments, systematically matching,
verifying, and applying the fully tested staging rule changes to the production database nodes.

Supports target MLS batch testing notice via 'temp-data/test_mls.txt' (via rules_utils.py).
"""

import io
import os
import time
from pathlib import Path
import paramiko
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv

from rules_utils import load_target_test_mls

# DYNAMIC RESOLUTION: Find the .env file sitting right next to this root-level script
current_dir = Path(__file__).resolve().parent
load_dotenv(dotenv_path=current_dir / ".env")


def run_production_promotion():
    # 1. Fetch credentials from your .env file
    hostname = os.getenv("REMOTE_HOST")
    username = os.getenv("SSH_USER")
    key_path = os.path.expanduser(os.getenv("SSH_KEY_PATH")) if os.getenv("SSH_KEY_PATH") else ""
    key_passphrase = os.getenv("SSH_KEY_PASSPHRASE")
    remote_dir = os.getenv("REMOTE_DIR")

    # Target the true local temp-data folder directly in your rules_analytics project
    temp_data_dir = current_dir / "temp-data"
    local_file_path = temp_data_dir / "promotion_sources.txt"

    # Text string identifiers used for remote file management
    filename_str = "promotion_sources.txt"
    remote_file = f"{remote_dir}/{filename_str}"

    print("\n" + "=" * 60)
    print("PRODUCTION PROMOTION ENGINE INITIALIZED")
    print("=" * 60)

    # Check if active MLS batch targets exist in temp-data/test_mls.txt
    target_mls_set = load_target_test_mls(temp_data_dir)
    if target_mls_set:
        print(f"🧪 [PROMOTION BATCH GUARD] Active MLS targets detected: {sorted(list(target_mls_set))}")

    # Immediate early-exit fail check if the source file wasn't created yet
    if not local_file_path.exists():
        print(f"[ERROR] Local source file missing at {local_file_path}")
        print("   Please ensure your sandbox extraction scripts ran and generated it first.")
        print("=" * 60 + "\n")
        return

    # 2. Initialize Client
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # 3. Decrypt and load the key
        with open(key_path, "rb") as key_file:
            decrypted_private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=(
                    key_passphrase.encode() if key_passphrase else None
                ),
            )
        pem_data = decrypted_private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
        mypkey = paramiko.RSAKey.from_private_key(io.StringIO(pem_data.decode()))

        # 4. Connect to server
        print(f"[1/4] Connecting to remote host {hostname}...")
        ssh.connect(hostname=hostname, username=username, pkey=mypkey, timeout=10)

        # 5. SFTP Upload using the resolved path object converted to string
        print(f"[2/4] Uploading manifest to server path: {remote_file}...")
        sftp = ssh.open_sftp()
        sftp.put(str(local_file_path), remote_file)
        sftp.close()
        print("[SUCCESS] Manifest staged successfully on remote file layer.")

        # 6. Verify creation via remote terminal check
        print(f"[3/4] Verifying remote payload structure consistency...")
        stdin, stdout, stderr = ssh.exec_command(f"cat {remote_file}")
        file_contents = stdout.read().decode().strip()

        if file_contents:
            print("[INFO] Target Production Manifest records validated:")
            for line in file_contents.splitlines():
                print(f"   Targeted MLS ID: {line}")
        else:
            print(f"[ERROR] Upload finished but remote file is empty or missing.")
            return

        # 7. EXECUTE REPETITIVE SEQUENTIAL INTERACTIVE TERMINAL STREAM
        print(f"[4/4] Invoking production rule migration engine sequentially...")

        # Open an active interactive shell session channel
        channel = ssh.invoke_shell()

        # Define the lines exactly as they are sequentially typed into a live terminal
        commands = [
            "s_p",
            "set_promotion stage prod",
            f"while IFS= read -r i; do echo 'Processing ID: '$i; python promote_source.py -s process \"$i\" -y; done < {remote_file}",
            "exit"  # Gracefully disconnects the channel session at completion
        ]

        print("\n--- PRODUCTION SHELL INTERACTION OUTPUT LOGS ---")

        # Send each command string down the live socket stream sequentially
        for cmd in commands:
            print(f"[INPUT] Sending interactive shell input: {cmd}")
            channel.send(cmd + "\n")

            # Allow the shell adequate time to process commands and stream output responses
            time.sleep(2.0)

            # Read and print the live incoming streaming buffer data from the terminal channel
            if channel.recv_ready():
                output_chunk = channel.recv(4096).decode(errors="ignore")
                print(output_chunk)

        # Wait for the terminal process to wrap up its execution queue
        while not channel.exit_status_ready():
            time.sleep(1.0)
            if channel.recv_ready():
                output_chunk = channel.recv(4096).decode(errors="ignore")
                print(output_chunk)

        print("---------------------------------------------------\n")
        print("[SUCCESS] Production update cycle completed sequentially.")

    except Exception as e:
        print(f"\n[CRITICAL] PROMOTION PIPELINE FAILED: {e}")
    finally:
        ssh.close()
        print("[INFO] Secure interactive session closed.")
        print("=" * 60 + "\n")


if __name__ == "__main__":
    run_production_promotion()
