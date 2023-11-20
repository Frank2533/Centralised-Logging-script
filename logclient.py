import socket
import Amazon_uae_assortment_variables as variables

script_type = variables.script_type
remote_server_ip = variables.log_server  # Replace with the actual IP address of the remote server
remote_server_port = int(variables.log_port)  # Replace with the actual port number used by the server
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket.connect((remote_server_ip, remote_server_port))
FORMAT ='utf-8'
HEADER = 8
msg = "PING".encode(FORMAT)
msg_length = len(msg)
send_length = str(msg_length).encode(FORMAT)
send_length += b' ' * (HEADER - len(send_length))
client_socket.send(send_length)
client_socket.send(msg)

def send_log_message(remote_server_ip, remote_server_port, message):
    # Create a socket and connect to the remote server
    # client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # client_socket.connect((remote_server_ip, remote_server_port))
    global client_socket

    try:
        # Send the log message to the server
        msg = message.encode(FORMAT)
        msg_length = len(msg)
        send_length = str(msg_length).encode(FORMAT)
        send_length += b' ' * (HEADER - len(send_length))
        client_socket.send(send_length)
        client_socket.send(msg)
        if '!DISCONNECT!' in message:
            client_socket.close()
        else:
            return

    except Exception as e:
        print(f"Error: {e}")

    # finally:


def send_log(logmessage, level):

    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    log_message = ip_address +":-:"+ level + ":-:" + logmessage
    send_log_message(remote_server_ip, remote_server_port, log_message)
    # log_message = "This is a test log message from the client.2"
    # send_log_message(remote_server_ip, remote_server_port, log_message)

# send_log("Hi a test msg", "info")