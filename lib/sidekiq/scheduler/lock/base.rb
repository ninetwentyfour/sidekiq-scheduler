module Sidekiq
  class Scheduler
    module Lock
      class Base
        attr_reader :key
        attr_accessor :timeout

        def initialize(key, options = {})
          @key = key

          # 3 minute default timeout
          @timeout = options[:timeout] || 60 * 3
        end

        # Attempts to acquire the lock. Returns true if successfully acquired.
        def acquire!
          raise NotImplementedError
        end

        def value
          @value ||= [hostname, process_id].join(':')
        end

        # Returns true if you currently hold the lock.
        def locked?
          raise NotImplementedError
        end

        # Releases the lock.
        def release!
          Sidekiq.redis{|r| r.del(key)} == 1
        end

      private

        # Extends the lock by `timeout` seconds.
        def extend_lock!
          Sidekiq.redis{|r| r.expire(key, timeout)}
        end

        def hostname
          local_hostname = Socket.gethostname
          Socket.gethostbyname(local_hostname).first rescue local_hostname
        end

        def process_id
          Process.pid
        end
      end
    end
  end
end
