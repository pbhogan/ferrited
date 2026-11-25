# frozen_string_literal: true

# Loaded first via RUBYOPT by tests/rails/run.sh. Rails' cache tests fall back to
# 127.0.0.1:11211 in places, where a developer's memcached may listen, so connections to it are
# refused and recorded in PORT_GUARD_LOG, which run.sh checks after each pass.

require "socket"

module PortGuard
  FORBIDDEN = [11211].freeze
  LOG = ENV.fetch("PORT_GUARD_LOG", "/dev/stderr")

  def self.check!(host, port, via)
    return unless FORBIDDEN.include?(Integer(port.to_s, exception: false))

    message = "PORT_GUARD: refused #{via} to #{host}:#{port}\n#{caller(2, 8).join("\n")}\n"
    File.open(LOG, "a") { |f| f.write(message) }
    raise Errno::ECONNREFUSED, "port guard refused #{host}:#{port}"
  end

  def self.check_sockaddr!(sockaddr, via)
    addr = sockaddr.is_a?(Addrinfo) ? sockaddr : Addrinfo.new(sockaddr)
    check!(addr.ip_address, addr.ip_port, via) if addr.ip?
  rescue ArgumentError, SocketError
    nil
  end

  # Not on TCPSocket#initialize: Dalli inspects its parameters to decide on connect_timeout:.
  module TCPSocketNew
    def new(host = nil, port = nil, *rest, **kw)
      PortGuard.check!(host, port, "TCPSocket.new")
      super
    end

    def open(host = nil, port = nil, *rest, **kw)
      PortGuard.check!(host, port, "TCPSocket.open")
      super
    end
  end

  module SocketTcp
    def tcp(host, port, *rest, **kw, &block)
      PortGuard.check!(host, port, "Socket.tcp")
      super
    end
  end

  module SocketConnect
    def connect(sockaddr)
      PortGuard.check_sockaddr!(sockaddr, "Socket#connect")
      super
    end

    def connect_nonblock(sockaddr, *rest, **kw)
      PortGuard.check_sockaddr!(sockaddr, "Socket#connect_nonblock")
      super
    end
  end
end

TCPSocket.singleton_class.prepend(PortGuard::TCPSocketNew)
Socket.singleton_class.prepend(PortGuard::SocketTcp)
Socket.prepend(PortGuard::SocketConnect)
