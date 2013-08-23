require 'yaml'
require 'keys'
require 'redis_wrapper/handler'
require 'redis_wrapper/auth'

module RedisWrapper

	# Making config to any class can access
	@config = YAML::load(IO.read("#{File.expand_path(File.dirname(__FILE__))<<"/../"}config/core.yml"))
	def self.config
		@config
	end


	class Server
		require 'redis'
		require 'hiredis'
		require 'socket'
		require 'json'

		attr_accessor :applications

		# Authentication of application
		def authenticate(secret_key)
			@application = JSON.parse(@r.get("apps:#{secret_key.gsub(/[^a-z0-9]/,'')}")||'{}')
			!@application.empty?
		end

		def initialize
			@config = RedisWrapper::config

			@r = Redis.new(:host=>@config['redis']['host'], :port=>@config['redis']['port'])

			# Reset all data if needed
			flush_and_load_applications if ARGV.include?('--reset')

			# And... Let's get this party started!
			server = TCPServer.open(RedisWrapper::config["daemon"]["port"])
			loop {
				# client = server.accept
				Thread.start(server.accept) do |client|
					authenticated = false
					
					while line = client.gets
						case
							# Authentication process
							when(!authenticated and line.match(/^(AUTHME:)(.+)$/)) then 
								key = line.gsub("AUTHME:",'').strip
								authenticated = authenticate(key)
								client.puts({:success=>authenticated, :errors=>(authenticated ? '' : "AUTH FAILED")}.to_json.to_s)
							# Commands handler 
							when(authenticated and !line.match(/^(AUTHME:)(.+)$/)) then
								client.puts RedisWrapper::Handler.new(line, @r).result.to_json
							# Un authenticated error
							when(!authenticated and !line.match(/^(AUTHME:)(.+)$/)) then
								client.puts({:success=>false,:errors=>"NOT AUTHENTICATED"}.to_json)
							else
								client.puts({:success=>false,:errors=>"SOMETHING WRONG"}.to_json)
						end

					end
					client.close

				end
			}

		end

		# Flushes all databases and creates application hash
		def flush_and_load_applications
			@r.flushall # Only hardcode baby!!!

			# Restoring all default applications 
			@config["defaults"]["applications"].each do |k,app|
				@r.set("apps:#{app['key']}", app.reject{|a,v| a.eql?('key') }.to_json )
			end
			# We need this! Yeah baby we need this
			@r.bgsave

			# Puting alert that database cleaned
			puts ""
			puts "ACHTUNG!: Database cleaned and default applications id loaded"
			puts ""
		end


	end

end


RedisWrapper::Server.new