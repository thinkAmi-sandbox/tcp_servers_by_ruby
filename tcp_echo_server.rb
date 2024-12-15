require 'socket'

PORT = '12345'

Socket.tcp_server_loop(PORT) do |sock, addr_info|
  p sock.class # => Socket
  message = sock.recv 1000
  p message # => "hello\n"
  p message.class #=> String

  sock.sendmsg message
ensure
  sock.close
end