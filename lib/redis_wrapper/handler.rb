module RedisWrapper

	class Handler


		# Connection to redis
		attr_accessor :r
		# Parsed string except command
		attr_accessor :pr
		# Result
		attr_accessor :result

		

		# Creates a connection and class a method depending oon transferred command
		# If command do not exists, returns {:success=>false, :error=>"Wrong command"}
		def initialize(line, db_connection)
			@r = db_connection
			@pr = parse_expressison(line)
			@result = begin
				# Little honeypod to send native redis commands
				send(pr[:command])
			rescue	NoMethodError
				{:success=>false, :error=>"Wrong command"}
			end
			@result
		end


		# Counts number of records
		def count
			{:success=>true, :nodes => r.zcard(make_set_to_sort)}
		end


		# Initializes a model in :tablename = {'attribute':'Class'}
		def initmodel 
			r.hmset(":#{pr[:table_name]}", pr[:nodes])
			{:success=>true}
		end

		# Delete functionality
		def delete
			pr[:ids].each{ |id| 
				r.hdel(pr[:table_name], id)
				r.srem("#{pr[:table_name]}:id",id)
			}
			# Unindexing
			unindex(ids)
		end

	
		# Main finder method
		def find
			# Generating sort params 
			sort_params = {
				:order=>"#{pr[:params][:direction].to_s.empty? ? "DESC" : pr[:params][:direction]} ALPHA",
				:by=>"#{pr[:table_name]}:*->#{pr[:params][:order]}",
				:limit=>(pr[:params][:limit].empty? ? [0, -1] : [pr[:params][:offset].to_i, pr[:params][:limit].to_i])
			}
			# Sorting and getting all finded records
			records = r.sort(make_set_to_sort, sort_params).map{|id| r.hgetall("#{pr[:table_name]}:#{id}").to_json}
			
			{:success=>true, :nodes => records}
		end


		# Unindex then index. Dump and simple
		def reindex id
			unindex id
			index id
		end

		# Indexes fields of record
		def index id
			types = r.hgetall(":#{pr[:table_name]}")
			r.hgetall("#{pr[:table_name]}:#{id}").each do |attribute,value|
				value.gsub!(' ','_')
				r.zadd("#{pr[:table_name]}:#{attribute}", value.score, id)
			end
		end

		# Unindexes fields of record
		def unindex ids
			ids = [ids] unless ids.is_a?(Array) 
			ids.each do |id|
				r.hgetall("#{pr[:table_name]}:#{id}").each do |attribute,value|
					r.zrem("#{pr[:table_name]}:#{attribute}", id)
				end
			end
		end

		# Main setter method
		def set
			# Increment id if needed
			pr[:params]['id'] = pr[:params]['id'] || (r.incr("last_#{pr[:table_name]}_id"); r.get("last_#{pr[:table_name]}_id"))
			# Storing a record it self
			r.hmset("#{pr[:table_name]}:#{pr[:params]['id']}",pr[:params].to_a.flatten)
			# ReIndexing
			reindex pr[:params]['id']
			{:success=>true, :object=>pr}
		end

		private
		# Here comes a magick!
		# Makes zinterstore by filters and returns a name of set with result ids
		def make_set_to_sort
			# Shortcut for table_name
			tn = pr[:table_name]

			# Creating ranges to intersect
			ranges = pr[:filters].map{ |at, v|
				value = v[1].map{|vl| vl.nil? ? "null" : (vl.is_a?(String) ? vl.gsub(' ','_') : vl) }
				case v[0] 
					when '>'
						["(#{value.first.score}", '+inf']
					when '>='
						["#{value.first.score}", '+inf']
					when '<'
						['-inf', "(#{value.last.score}"]
					when '<='
						['-inf', "#{value.last.score}"]
					when '<>'
						["(#{value.first.score}", "(#{value.last.score}"]
					when '<=>'	
						[value.first.score, value.last.score]
					else
						[value.first.score, value.last.score]
					end
			}.flatten

			# Calculating set to sort
			sort_set = if pr[:filters].any?
				# Storing this all to to temp zset
				r.zinterstore('tmp:to_sort', pr[:filters].map{|k,v| "#{tn}:#{k}"}, :ranges=>ranges)
				# And returning name of this set
				'tmp:to_sort'
			else
				# Or just all ids 
				"#{tn}:id"
			end
			sort_set
		end

		# Parses an incoming expression 
		def parse_expressison line
			m = line.match(/^([^ ]+) ([^ ]+) (.+)$/i)
			result = {:command => m[1].to_s.downcase, :table_name => m[2]}
			line = m[3].to_s

			case result[:command]
				when 'initmodel' 
					m = line.match(/^[\s]*({[^}]+})$/i)
					result.merge!({:nodes => JSON.parse(m[1]).to_a.flatten})

				when 'find' || 'count'
					m = line.match(/^[\s]*([^ ]+) ({[^}]*}) ({[^}]+})$/i)
					result.merge!({
						:number=>m[1], 
						:filters=>JSON.parse(m[2]).symbolize_keys!, 
						:params=>JSON.parse(m[3]).symbolize_keys!
					})
				when 'set'
					m = line.match(/^[\s]*({[^}]+})$/i)
					result.merge!({
						:params => JSON.parse(m[1]).symbolize_keys!
					})
				when 'delete'
					m = line.match(/^[\s]*(.+)/i)
					result.merge!({
						:ids => JSON.parse(m[1]).symbolize_keys!
					})
			end
			result
		end

		
	end
end