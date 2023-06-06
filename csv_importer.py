# import_csv.py

import getpass
from paramiko import Transport, SFTPClient


def upload_file(
    local_filepath, remote_filepath, sftp_host, sftp_port, sftp_user, sftp_password
):
    # Set up the SFTP connection
    transport = Transport((sftp_host, sftp_port))
    transport.connect(username=sftp_user, password=sftp_password)

    sftp = SFTPClient.from_transport(transport)

    # Upload the file
    sftp.put(local_filepath, remote_filepath)

    print(f"File uploaded successfully to {remote_filepath} on {sftp_host}.")


def get_user_input():
    local_filepath = input("Please enter the full path to your local CSV file: ")
    sftp_host = input("SFTP host (IP address of the server): ")
    sftp_port = int(input("SFTP port: "))
    sftp_user = input("SFTP username: ")

    # getpass allows you to securely input the password without displaying it
    sftp_password = getpass.getpass("SFTP password: ")

    return local_filepath, sftp_host, sftp_port, sftp_user, sftp_password


def main():
    local_filepath, sftp_host, sftp_port, sftp_user, sftp_password = get_user_input()
    remote_filepath = "/home/tadeas/nicolh/csvs/" + local_filepath.split("/")[-1]
    upload_file(
        local_filepath, remote_filepath, sftp_host, sftp_port, sftp_user, sftp_password
    )


if __name__ == "__main__":
    main()
