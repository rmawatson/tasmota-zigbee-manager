import multiprocessing
import sys
from ftplib import FTP
import time
from pathlib import Path
import argparse

def ftp_operation(file_name, ip_address):

    if file_name.startswith('.\\'):
        file_name = file_name[2:]
        
    file_name = file_name
    target_filename = ".extensions/" + Path(file_name).name
    try:
        ftp = FTP(ip_address)
        ftp.set_pasv(False) 
        ftp.connect()
        try:
            ftp.delete(file_name)
            print(f'Deleted existing {file_name}')
        except:
            print(f'{file_name} did not exist')
        

        try:
            with open(file_name, 'rb') as file:
                ftp.storbinary(f'STOR {target_filename}', file)
            
            ftp.retrlines('LIST', print)
            ftp.quit()
        except:
            print("Error")
            sys.exit(1)
        print("FTP operation completed successfully")
        return True
    except Exception as e:
        print(f"Error during FTP operation: {e}")
        sys.exit(1)

def run_with_timeout(file_name, ip_address, timeout=6, max_retries=8):
    for attempt in range(1, max_retries + 1):
        print(f"\nAttempt {attempt}/{max_retries}")
        process = multiprocessing.Process(target=ftp_operation, args=(file_name, ip_address))
        process.start()
        process.join(timeout=timeout)
        if process.is_alive():
            print(f"Process timed out (>{timeout}s). Killing process...")
            process.terminate()
            process.join()
        elif process.exitcode == 0:
            print("Process completed successfully")
            break
    else:
        print(f"Failed to complete after {max_retries} attempts")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Upload files to FTP server with timeout and retry logic')
    parser.add_argument('--filename', required=True, help='Path to the file to upload')
    parser.add_argument('--ip-address', default='10.0.0.182', help='IP address of the FTP server (default: 10.0.0.182)')
    
    args = parser.parse_args()
    
    run_with_timeout(args.filename, args.ip_address)