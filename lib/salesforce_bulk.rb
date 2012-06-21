require 'net/https'
require 'xmlsimple'
require 'csv'
require "salesforce_bulk/version"
require 'salesforce_bulk/job'
require 'salesforce_bulk/connection'

module SalesforceBulk
  class Api
    @@SALESFORCE_API_VERSION = '24.0'

    DEFAULT_OPTIONS = {
      :sandbox => false,
      :check_interval => 10,
      :logger => nil
    }

    def initialize(username, password, options = {})
      @options = DEFAULT_OPTIONS.merge options
      @logger = @options[:logger]
      @connection = SalesforceBulk::Connection.new(username, password, @@SALESFORCE_API_VERSION, @options)
    end

    def upsert(sobject, records, external_field, wait=false)
      self.do_operation('upsert', sobject, records, external_field, wait)
    end

    def update(sobject, records)
      self.do_operation('update', sobject, records, nil)
    end
    
    def create(sobject, records)
      self.do_operation('insert', sobject, records, nil)
    end

    def delete(sobject, records)
      self.do_operation('delete', sobject, records, nil)
    end

    def query(sobject, query, &block)
      self.do_operation('query', sobject, query, nil, &block)
    end

    def do_operation(operation, sobject, records, external_field, wait=false, &block)
      job = SalesforceBulk::Job.new(operation, sobject, records, external_field, @connection, @options)

      # TODO: put this in one function
      job_id = job.create_job()
      if(operation == "query")
        batch_id = job.add_query()
      else
        batch_id = job.add_batch()
      end
      job.close_job()

      if wait or operation == 'query'
        while true
          state = job.check_batch_status()
          @logger.debug "state is #{state}" if @logger
          if state != "Queued" && state != "InProgress"
            break
          end
          @logger.debug "waiting for #{@options[:check_interval]}" if @logger
          sleep(@options[:check_interval]) # wait x seconds and check again
        end
        
        if state == 'Completed'
          @logger.debug "#{File.basename __FILE__}:#{__LINE__}: fetching result" if @logger
          job.get_batch_result(&block)
        else
          return "There is an error in your job."
        end
      else
        return "The job has been closed."
      end
    end
  end  # End class
end # End module
