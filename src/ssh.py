from sshtunnel import SSHTunnelForwarder

server = SSHTunnelForwarder(
     ('165.22.198.35', 22),
     ssh_password="EP3rFnJYXM",
     ssh_username="erfan",
     ssh_private_key=r'C:\Users\ErfanNarimanVeneficu\.ssh\bricc',
     remote_bind_address=('127.0.0.1', 3306)
)
