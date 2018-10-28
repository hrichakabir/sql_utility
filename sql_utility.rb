require 'net/http'
require 'mysql'

class SQLUtil
  MAX_INSERTION = 500

  def initialize(username, password, host, db_name)
      #my = Mysql.new(hostname, username, password, databasename)
      @client = Mysql.new(host, username, password, db_name)
  end

  def close_client
    @client.close
  end

  def fetch(table_name)
    sql_str = "SELECT * FROM #{table_name}"
    begin
      res = @client.query(sql_str)
    rescue => e
      puts "Unable to fetch from #{table_name} : #{e}"
    end
  end

  def bulk_insert(table_name, rows)
    return false if !rows.kind_of?(Array) || rows.count == 0
    inserted = true
    sql_str = ""
    col_names = rows.first.map {|k,v| k}
    sql_col_names = col_names.map {|col_name| "`#{col_name}`"}
    sql_insert_str = "INSERT INTO #{table_name} (#{sql_col_names.join(',')}) values"

    rows_count = rows.count
    rows.each_with_index do |row, index|
      col_values = []
      col_names.each {|col_name| col_values.push(row[col_name])}
      if sql_str.empty?
        sql_str = "#{sql_insert_str}(#{get_col_values_str(col_values)})"
      else
        sql_str += ",(#{get_col_values_str(col_values)})"
      end
      if ((index+1) % MAX_INSERTION) == 0 || rows_count == (index+1)
        inserted = execute_query(sql_str)
        sql_str = ""
      end
    end
    return inserted
  end

  def bulk_update(table_name, rows, unique_key_columns)
    return false if !rows.kind_of?(Array) || rows.count == 0
    updated = true
    rows.each do |row|
      sql_str = ""
      sql_str = "#{get_update_str(table_name, row, unique_key_columns)}"
      updated = execute_query(sql_str)
    end
    return updated
  end

  def bulk_delete(table_name, key_name, key_values)
    return false if !key_values.kind_of?(Array) || key_values.count == 0
    deleted = true
    delete_sql_str = "DELETE FROM #{table_name} where #{key_name} in "
    values_str = ""
    values_count = key_values.count
    key_values.each_with_index do |value, index|
      values_str += ',' unless values_str.empty?
      values_str += "'#{value}'"
      if values_str.length >= 100 || values_count == (index+1)
        sql_str = "#{delete_sql_str} (#{values_str})"
        deleted = execute_query(sql_str)
      end
    end
    return deleted
  end

  def execute_query(sql_str)
    begin
      res = @client.query(sql_str)
    rescue => e
      puts "Error in executing query #{sql_str} :: #{e}"
      return false
    end
    return true
  end

  def get_col_values_str(col_values)
    str = ""
    col_values.each_with_index do |val, i|
      str += "," if i > 0
      str += get_col_value_str(val)
    end
    return str
  end

  def get_col_value_str(col_value)
    if col_value.kind_of?(Time)
      col_value_str = "'#{col_value.strftime("%Y-%m-%d")}'"
    elsif col_value.kind_of?(String)
      col_value_str = "'#{col_value}'"
    else
      col_value_str = "#{col_value}"
    end
    return col_value_str
  end

  def get_update_str(table_name, col_values_hash, unique_key_columns)
    return "" if !col_values_hash.kind_of?(Hash) || col_values_hash.count == 0
    str = "UPDATE #{table_name} SET "
    i=0
    col_values_hash.each do |col_name, col_value|
      if !unique_key_columns.include?(col_name)
        str += ',' if i>0
        str += "`#{col_name}` = #{get_col_value_str(col_value)}"
        i += 1
      end
    end
    str += " where "
    unique_key_columns.each_with_index do |col_name, index|
      str += " and " if index > 0
      str += "`#{col_name}` = #{get_col_value_str(col_values_hash[col_name])}"
    end
    str += ";"
    return str
  end

end
