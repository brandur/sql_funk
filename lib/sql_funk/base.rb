# SqlFunk

require 'active_record'

module SqlFunk
  # def self.included(base)
  #   base.send :extend, ClassMethods
  # end
  
  module Base
    
    def aggregate_by(options = {})
      raise ArgumentError, ':aggregate is required'   unless options[:aggregate]
      raise ArgumentError, ':date_column is required' unless options[:date_column]

      options[:aggregate]     ||= 'COUNT(*)'
      options[:aggregate_key] ||= options[:aggregate]
      options[:order]         ||= 'ASC'
      options[:group_by]      ||= 'day'
      options[:group_column]  ||= options[:group_by]

      date_func = case options[:group_by]
      when "day"
        case ActiveRecord::Base.connection.adapter_name.downcase
        when /^sqlite/ then "STRFTIME(\"%Y-%m-%d\", #{options[:date_column]})"
        when /^mysql/ then "DATE(#{options[:date_column]})"
        when /^postgresql/ then "DATE_TRUNC('day', #{options[:date_column]})"
        end
      when "month"
        case ActiveRecord::Base.connection.adapter_name.downcase
        when /^sqlite/ then "STRFTIME(\"%Y-%m\", #{options[:date_column]})"
        when /^mysql/ then "DATE_FORMAT(#{options[:date_column]}, \"%Y-%m\")"
        when /^postgresql/ then "DATE_TRUNC('month', #{options[:date_column]})"
        end
      when "year"
        case ActiveRecord::Base.connection.adapter_name.downcase
        when /^sqlite/ then "STRFTIME(\"%Y\", #{options[:date_column]})"
        when /^mysql/ then "DATE_FORMAT(#{options[:date_column]}, \"%Y\")"
        when /^postgresql/ then "DATE_TRUNC('year', #{options[:date_column]})"
        end
      end

      self.select("#{date_func} AS #{options[:group_column]}, #{options[:aggregate]} AS #{options[:aggregate_key]}").group(options[:group_column]).order("#{date_func} #{options[:order]}")
    end
    
    def count_by(date_column, options = {})
      options[:aggregate]     = 'COUNT(*)'
      options[:aggregate_key] = 'count_all'
      options[:date_column]   = date_column
      aggregate_by(options)
    end
    # 
    # def method_missing(id, *args, &block)
    # 
    #   return count_by(args[0], { :group_by => "day" }.merge(args[1]))
    # 
    #   # return count_by(args[0], { :group_by => "day" }.merge(args[1])) if id.id2name == /count_by_day/
    #   #     
    #   # return count_by(args[0], { :group_by => Regexp.last_match(1) }.merge(args[1])) if id.id2name =~ /count_by_(.+)/
    # 
    # end
    
  end

end
