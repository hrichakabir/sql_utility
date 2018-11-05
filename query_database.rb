require './sql_utility.rb'

require 'yaml'

TABLE_NAME = 'employee'

def query_database()
  config = YAML.load_file(File.join(__dir__, 'config.yml'))
  #create an instance of sql utility
  sql_adapter = SQLUtil.new(config)

  #create table in database
  puts "\n#### Create table in database ####"
  res = sql_adapter.execute_query("CREATE TABLE IF NOT EXISTS #{TABLE_NAME} (id VARCHAR(50) PRIMARY KEY NOT NULL, name VARCHAR(200), city VARCHAR(200), job VARCHAR(50) NOT NULL)")
  if res
    puts "#{TABLE_NAME} created successfully"
  else
    puts "Error in table #{TABLE_NAME} creation."
  end

  #insert records into table
  puts "\n#### Insert records in table ####"
  rows = [{'id': '1', 'name': 'Hricha', 'city': 'Pune', 'job': 'IT'}, {'id': '2', 'name': 'John', 'city': 'Sydney', 'job': 'Media'}]
  result, error = sql_adapter.bulk_insert(TABLE_NAME, rows)
  if result
    puts "Record(s) inserted into table #{TABLE_NAME} successfully."
  else
    puts "Error in inserting records #{error}."
  end

  #fetch records from table
  puts "\n#### Fetch records from table ####"
  result, error = sql_adapter.fetch(TABLE_NAME)
  if error.nil?
    result.each do |row| puts row end
  else
    puts "Error in fetching records #{error}."
  end

  #update records
  puts "\n#### Update records in table ####"
  rows = [{'id': '1', 'name': 'Hricha Kabir'}, {'id': '2', 'job': 'Software1'}]
  unique_key_columns = [:id]
  result, error = sql_adapter.bulk_update(TABLE_NAME, rows, unique_key_columns)
  if result
    puts "Record(s) updated into table #{TABLE_NAME} successfully."
  else
    puts "Error in updating records #{error}."
  end

  #delete records
  puts "\n#### Delete records from table ####"
  result = sql_adapter.bulk_delete(TABLE_NAME, 'id', ['1','2', '3'])
  if result
    puts "Record(s) deleted from table #{TABLE_NAME} successfully."
  else
    puts "Error in deleting records #{error}."
  end

  puts "\n#### Close connection #####"
  sql_adapter.close_client()
end

query_database()
