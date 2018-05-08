class Upsert
  class Connection
    # @private
    class PG_Connection < Connection
      include Postgresql
      
      def execute(sql, params = nil)
        if sql.include?("$$replace$$")
          sql = sql.gsub("$$replace$$", "#{Apartment::Tenant.current}")
        end
        if params
          # Upsert.logger.debug { %{[upsert] #{sql} with #{params.inspect}} }
          metal.exec sql, convert_binary(params)
        else
          Upsert.logger.debug { %{[upsert] #{sql}} }
          metal.exec sql
        end
      end

      def quote_ident(k)
        if k.include?("$$replace$$")
          metal.quote_ident k.to_s.gsub("$$replace$$", "#{Apartment::Tenant.current}")
        else  
          metal.quote_ident k.to_s
        end
      end

      def binary(v)
        { :value => v.value, :format => 1 }
      end
    end
  end
end
