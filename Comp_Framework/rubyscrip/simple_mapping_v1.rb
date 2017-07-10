#/bin/ruby
#encoding:UTF-8

#simple_mapping.rb v0.1
slog = ''
slog = slog+ "Running Simple_mapping.rb..." + "\n"

#loops through control files
	if ARGV.empty?
		slog = slog+ 'Specify a parameter.ctrl file'  + "\n"
		exit
	end
	
	slog = slog+ 'started :' +Time.now.to_s + "\n"
	
	ARGV.each do |control_file|
		slog = slog+ '... for' +control_file  + "\n"
		source_file_name = ''
		target_file_name = ''
		source_delim = ''
		target_delim = ''
		expected_source_fields_count = 0
		expected_target_fields_count = 0
		load_keys = false
		load_mappings = false
		keys = []
		mapping_texts = []
		longest_mapping_text = 0
		mappings = []
		mapping_failures = []
		c = File.new('C:/cygwin/home/KV/control/' + control_file,"r")
		c.each_line do |control_line|
			control_line.chomp!
			#control_line.encode !('UTF-8','UTF-16',:invalid=>:replace,:replace =>'')
			#control_line.encode!('UTF-8','UTF-16')
			next if control_line =~/^#/
		 		if control_line =~ /^source_file/ 
					source_file_name = control_line.split('|').last
				end
				if control_line =~ /^target_file/
					target_file_name = control_line.split('|').last
				end
				if control_line =~ /^source_delim/
					source_delim = control_line.split(':').last
				end
				if control_line =~ /^target_delim/
					target_delim = control_line.split(':').last
				end
				if control_line =~ /^keys/
					load_keys = true
					next
				end
				if control_line =~ /^mappings/
					load_keys = false
					load_mappings = true
					next
				end
				if load_keys
					keys = keys.push( '(' + control_line + ')' )
				end
				
				if load_mappings
					mapping_texts.push(control_line)
					longest_mapping_text = control_line.length if longest_mapping_text < control_line.length
					mappings.push( '(' + control_line.split('#').first + ')' )
					mapping_failures.push(0)
				end
			end 

			c.close
		
		#load source & target into memory
		slog = slog+ "load sorted source"  + "\n"
		#slog = slog+ source_file_name  
		source_file = File.new(source_file_name,"r")
		source_records = []
		source_file.each_line do |source_record|
			source_record.chomp!
			source_record.gsub!("\xEF\xBB\xBF",'')#DELETES CHARS
			#source_record.encode!('UTF-16','UTF-8',:invalid =>:replace,:replace=>'')
			#source_record.encode!('UTF-8','UTF-16')
			source_records.push(source_record)
			end
	
		source_file.close
		number_of_source_records = source_records.size
		  
		#---------Added after error
		slog = slog+ "load sorted target"  + "\n"
		  
		target_file = File.new(target_file_name,"r")
		target_records = []
		target_file.each_line do |target_record|
			target_record.chomp!
			target_record.gsub!("\xEF\xBB\xBF",'')#DELETES CHARS
			#target_record.encode!('UTF-16','UTF-8',:invalid =>:replace,:replace=>'')
			#target_record.encode!('UTF-8','UTF-16')
			target_records.push(target_record)	
			end
	
		target_file.close
		number_of_target_records = target_records.size
	
		#loop through each record.
		
		slog = slog+ "Loop through source ,checking for target ..." + "\n"
		source_record_count = 0
		failed_mapping_count = 0
		missing_target_records = 0
		missing_source_records = 0
	
		for source_record in source_records

			source_record_count +=1
	  	    s = source_record.encode('UTF-8','binary',:invalid => :replace,:undef => :replace).split(source_delim)
 
			#-------------------------------------------
			#loop through target until file match
			#-------------------------------------------
			
			target_record_count = 0
			for target_record in target_records

				target_record_count += 1		   
				t = target_record.encode('UTF-8','binary',:invalid=>:replace,:undef =>:replace).split(target_delim)
				
				#-------------------------------------------
				#check for matching record
				#-------------------------------------------
	 
				if eval(keys.join(' && '))#match on key fields
					mapped = true
					i = 0
					
					for mapping in mappings
						i += 1
					 
						unless eval( mapping )
							failed_mapping_count += 1
							mapping_failures[i - 1] += 1
							slog = slog+ "------------" + "\n"
							slog = slog+ " " + "\n"
							slog = slog+ "failed mapping source record #{source_record_count} to target record" + "\n"
							slog = slog+ "-" *(( 46 + source_record_count.to_s.length ) + ( 1 + target_record_count.to_s.length )) + "\n"
							slog = slog+ " " + "\n"
							slog = slog+ "Source_record :\n #{source_record}" + "\n"
							slog = slog+ " "
							slog = slog+ "target_record :\n #{target_record}" + "\n"
							slog = slog+ " " + "\n" 
							lhs = (mapping.split('==').first).to_s + ')'
							rhs = '(' + (mapping.split('==').last).to_s
							slog = slog+ mapping_texts[ i-1 ]+ ':' + eval( lhs ).to_s + ' != ' + eval( rhs ).to_s + "\n"
							slog = slog+ " " + "\n"
						end 
					end 
				
					target_records.delete_at( target_record_count - 1  ) #remove found target record
					#target_record_count += 1
					break # move on to next source record 
				end
			
				slog = slog+ "tc #{target_record_count}" + "\n"
				slog = slog+ "ts #{target_records.size}" + "\n"
			
				if target_record_count == target_records.size
					slog = slog+"Ran out of target records for source records #{source_record_count}!" + "\n"
					slog = slog+ source_record + "\n"
					missing_target_records += 1
				end
			end
			#------- To Print Records Present in Source But Missing in Target
			#if target_record_count == target_records.size
			#	slog = slog+"Ran out of target records for source records #{source_record_count}!"
			#	slog = slog+ source_record
			#	missing_source_records += 1
			#end
		end

		slog = slog+ " " + "\n"
		slog = slog+ "Summary" + "\n"
		slog = slog+ "=================================================" + "\n"
		slog = slog+ "processed #{control_file} :#{source_file_name} with #{source_record_count} source_records" + "\n"
		slog = slog+ "processed #{control_file} : #{target_file_name} with #{number_of_target_records} target records " + "\n"
		slog = slog+ " " + "\n"
		 
		slog = slog+ "Mismatch in record count : " + '	' + (number_of_target_records - number_of_source_records).to_s.rjust(6)   + "\n"
		slog = slog+ " " + "\n"
		#slog = slog+ "Missing source records : " + '	'  + target_records.size.to_s.rjust(6) 
		slog = slog+ "Missing source records : " + '	'  + missing_source_records.to_s.rjust(6)  + "\n"
		slog = slog+ " " + "\n"
		slog = slog+ "Missing target records : " + '	' + missing_target_records.to_s.rjust(6)   + "\n"
		slog = slog+ " " + "\n"
		i=0
		left_column_width=longest_mapping_text + 1 
		slog = slog+ '	Mapping'.ljust(left_column_width) + '		' + 'failed'.rjust(9)  + "\n"
		slog = slog+ '---------------'.ljust(left_column_width)+ '		' + '--------------'.rjust(9) + "\n"
			for mapping_text in mapping_texts
			  rhs=mapping_texts[i].split('#').last
				#slog = slog+ (i + 1).to_s.rjust(2) + '.' +  rhs.ljust(left_column_width) + mapping_failures[i].to_s.rjust(9)  
				slog = slog+ (i+1).to_s.rjust(2)+ '.' +  rhs.ljust(left_column_width) + '		' + mapping_failures[i].to_s.rjust(9) + "\n"
						i +=1
						
			end

		slog = slog+ '' .ljust(left_column_width) + '		'  + '	==========='   + "\n"
		slog = slog+ 'Totals' .ljust(left_column_width)+ '		' + failed_mapping_count.to_s.rjust(9)  + "\n"

			if target_records.size > 0
				slog = slog+ " " + "\n"
				slog = slog+ " There are #{target_records.size} target records that could not be found in the source :" + "\n"
				slog = slog+ "-------------------------------------"+("-" * target_records.size.to_s.length) + "\n"
				i=0
				for target_record in target_records
					i+=1
					slog = slog+ i.to_s + '.' + target_record + "\n"
					slog = slog+ " " + "\n"
				end
			end
		slog = slog+ " " + "\n"
		slog = slog+  'completed:' +Time.now.to_s 
		puts slog
		
	end
	return slog
	