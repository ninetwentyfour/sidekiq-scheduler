require 'sidekiq/scheduler/lock/base'

module Sidekiq
  class Scheduler
    module Lock
      class Basic < Base
        def acquire!
          if Sidekiq.redis.setnx(key, value)
            extend_lock!
            true
          end
        end

        def locked?
          if Sidekiq.redis.get(key) == value
            extend_lock!

            if Sidekiq.redis.get(key) == value
              return true
            end
          end

          false
        end
      end
    end
  end
end
