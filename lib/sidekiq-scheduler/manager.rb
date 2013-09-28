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
      logger.info "STOP CALLED"
      @enabled = false
      # Thread.new { release_master_lock! }
    end

    def start
      logger.info "Starting scheduler"
      # register_signal_handlers
      #Load the schedule into rufus
      #If dynamic is set, load that schedule otherwise use normal load
      # if is_master?
        # logger.info "IS MASTER"
        if @enabled && Sidekiq::Scheduler.dynamic
          Sidekiq::Scheduler.reload_schedule!
        elsif @enabled
          Sidekiq::Scheduler.load_schedule!
        end
      # end
    end

    def reset
      clear_scheduled_work
    end

    # # For all signals, set the shutdown flag and wait for current
    # # poll/enqueing to finish (should be almost istant).  In the
    # # case of sleeping, exit immediately.
    # def register_signal_handlers
    #   logger.info "caught signal"
    #   trap("TERM") { shutdown }
    #   trap("INT") { shutdown }

    #   begin
    #     trap('QUIT') { shutdown }
    #     # trap('USR1') { print_schedule }
    #     # trap('USR2') { reload_schedule! }
    #   rescue ArgumentError
    #     warn "Signals QUIT and USR1 and USR2 not supported."
    #   end
    # end

    # # Sets the shutdown flag, clean schedules and exits if sleeping
    # def shutdown
    #   @shutdown = true

    #   # if @sleeping
    #     # Resque.clean_schedules
    #     Thread.new { release_master_lock! }
    #     exit
    #   # end
    # end

  end

end