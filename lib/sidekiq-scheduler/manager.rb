require 'celluloid'
require 'redis'
require 'multi_json'

require 'sidekiq/util'

require 'sidekiq/scheduler'
require 'sidekiq/scheduler_locking'
require 'sidekiq-scheduler/schedule'

module SidekiqScheduler

  # The delayed job router in the system.  This
  # manages the scheduled jobs pushed messages
  # from Redis onto the work queues
  #
  class Manager
    include Sidekiq::Util
    include Sidekiq::SchedulerLocking
    include Celluloid

    def initialize(options={})
      @enabled = options[:scheduler]

      Sidekiq::Scheduler.dynamic = options[:dynamic] || true
      Sidekiq.schedule = options[:schedule] if options[:schedule]
    end

    def stop
      @enabled = false
    end

    def start
      logger.info "Starting scheduler"
      if @enabled && Sidekiq::Scheduler.dynamic
        Sidekiq::Scheduler.reload_schedule!
      elsif @enabled
        Sidekiq::Scheduler.load_schedule!
      end
    end

    def reset
      clear_scheduled_work
    end

  end

end