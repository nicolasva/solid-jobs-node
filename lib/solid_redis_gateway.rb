# frozen_string_literal: true

# Sidecar gateway: exposes the Ruby `solid-redis` gem to Python over a Unix
# socket. Wire protocol: one JSON object per line, one JSON reply per request.
#
#   {"command": "RPOP", "args": ["queue"]}              -> {"status": "OK", "data": ...}
#   {"pipeline": [["SET", "k", "v"], ["GET", "k"]]}      -> {"status": "OK", "data": [...]}
#   failure                                              -> {"status": "ERROR", "error": {"class": ..., "message": ...}}
#
# Every accepted socket connection owns one dedicated SolidRedis::Client, so
# requests of one connection are strictly ordered and connections are parallel.

require "socket"
require "json"
require "optparse"
require "solid_redis"

MAX_LINE_BYTES = 64 * 1024 * 1024

options = {}
OptionParser.new do |parser|
  parser.on("--socket PATH") { |value| options[:socket] = value }
  parser.on("--redis-url URL") { |value| options[:redis_url] = value }
end.parse!

socket_path = options.fetch(:socket)
redis_config = SolidRedis.config(url: options.fetch(:redis_url, "redis://127.0.0.1:6379/0"))

def utf8(value)
  case value
  when String then value.dup.force_encoding(Encoding::UTF_8).scrub
  when Array then value.map { |item| utf8(item) }
  when Hash then value.to_h { |key, item| [utf8(key), utf8(item)] }
  when Float then value.finite? ? value : value.to_s
  when StandardError then utf8(value.message)
  else value
  end
end

def command_of(entry)
  list = entry.is_a?(Hash) ? [entry.fetch("command"), *Array(entry["args"])] : Array(entry)
  raise ArgumentError, "empty command" if list.empty?

  list.map(&:to_s)
end

def reply(socket, payload)
  socket.write(JSON.generate(payload) << "\n")
end

def error_payload(error)
  {
    "status" => "ERROR",
    "error" => {"class" => error.class.name.to_s.split("::").last, "message" => utf8(error.message.to_s)},
  }
end

def serve(socket, redis_config)
  client = redis_config.new_client
  while (line = socket.gets("\n", MAX_LINE_BYTES))
    begin
      request = JSON.parse(line)
      data = if request.key?("pipeline")
        commands = request.fetch("pipeline").map { |entry| command_of(entry) }
        client.pipelined { |pipeline| commands.each { |command| pipeline.call_v(command) } }
      else
        client.call_v(command_of(request))
      end
      reply(socket, {"status" => "OK", "data" => utf8(data)})
    rescue JSON::ParserError, ArgumentError, KeyError, SolidRedis::Error => error
      reply(socket, error_payload(error))
    end
  end
rescue IOError, SystemCallError
  nil
ensure
  client&.close
  socket.close unless socket.closed?
end

File.unlink(socket_path) if File.exist?(socket_path)
previous_umask = File.umask(0o177)
server = UNIXServer.new(socket_path)
File.umask(previous_umask)
File.chmod(0o600, socket_path)

# The owning Python process holds our stdin; its death (EOF) stops the gateway.
Thread.new do
  $stdin.read
  server.close unless server.closed?
  File.unlink(socket_path) if File.exist?(socket_path)
  exit!(0)
end

trap("TERM") { exit!(0) }
trap("INT") { exit!(0) }

$stdout.puts("READY")
$stdout.flush

loop do
  connection = server.accept
  Thread.new(connection) { |socket| serve(socket, redis_config) }
rescue IOError, Errno::EBADF
  break
end
