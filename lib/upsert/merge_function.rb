require 'zlib'
require 'upsert/version'

class Upsert
  # @private
  class MergeFunction
    MAX_NAME_LENGTH = 62
    NAME_PREFIX = "upsert#{Upsert::VERSION.gsub('.', '_')}"

    class << self
      def unique_name(table_name, selector_keys, setter_keys, using_replace = true)
        prefix = (using_replace ? "$$replace$$" : "public")
        parts = [
          "#{prefix}.c#{Apartment::Tenant.current}",
          NAME_PREFIX,
          table_name,
          'SEL',
          selector_keys.join('_A_').gsub(" ","_"),
          'SET',
          setter_keys.join('_A_').gsub(" ","_")
        ].join('_')
        if parts.length > MAX_NAME_LENGTH
          # maybe i should md5 instead
          crc32 = Zlib.crc32(parts).to_s
          [ parts[0..MAX_NAME_LENGTH-10], crc32 ].join
        else
          parts
        end
      end
      
      def lookup(controller, row)
        @lookup ||= {}
        selector_keys = row.selector.keys
        setter_keys = row.setter.keys
        key = [controller.connection.metal.to_s, controller.table_name, selector_keys, setter_keys]
        @lookup[key] ||= new(controller, selector_keys, setter_keys, controller.assume_function_exists?)
      end
    end

    attr_reader :controller
    attr_reader :selector_keys
    attr_reader :setter_keys

    def initialize(controller, selector_keys, setter_keys, assume_function_exists)
      @controller = controller
      @selector_keys = selector_keys
      @setter_keys = setter_keys
      validate!
      create! unless assume_function_exists
    end

    def name
      l_table_name = table_name.split(".").last
      using_replace = table_name.split(".").length > 1
      @name ||= self.class.unique_name l_table_name, selector_keys, setter_keys, using_replace
    end

    def connection
      controller.connection
    end

    def table_name
      controller.table_name.gsub("$$replace$$", "#{Apartment::Tenant.current}")
    end

    def quoted_table_name
      controller.quoted_table_name.gsub("$$replace$$", "#{Apartment::Tenant.current}")
    end

    def column_definitions
      controller.column_definitions
    end

    private

    def validate!
      possible = column_definitions.map(&:name)
      invalid = (setter_keys + selector_keys).uniq - possible
      if invalid.any?
        raise ArgumentError, "[Upsert] Invalid column(s): #{invalid.map(&:inspect).join(', ')}"
      end
    end
  end
end
