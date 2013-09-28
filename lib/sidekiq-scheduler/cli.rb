require 'sidekiq-scheduler/manager'
require 'sidekiq'
require 'sidekiq/scheduler_locking'
require 'sidekiq/cli'

module SidekiqScheduler
  module CLI
    def self.included(base)
      base.class_eval do
        include Sidekiq::SchedulerLocking
        def run
          if is_master?
            logger.info "IS MASTER"
            scheduler_options = { :scheduler => true, :schedule => nil }
            scheduler_options.merge!(options)

            if options[:config_file]
              file_options = YAML.load_file(options[:config_file])
              options.merge!(file_options)
              options.delete(:config_file)
              parse_queues(options, options.delete(:queues) || [])
            end

            scheduler = SidekiqScheduler::Manager.new(scheduler_options)
            scheduler.start
          end

          self_read, self_write = IO.pipe

          %w(INT TERM USR1 USR2 TTIN).each do |sig|
            trap sig do
              self_write.puts(sig)
            end
          end

          redis {} # noop to connect redis and print info
          logger.info "Running in #{RUBY_DESCRIPTION}"
          logger.info Sidekiq::LICENSE

          if !options[:daemon]
            logger.info 'Starting processing, hit Ctrl-C to stop'
          end

          require 'sidekiq/launcher'
          @launcher = Sidekiq::Launcher.new(options)
          launcher.procline(options[:tag] ? "#{options[:tag]} " : '')

          begin
            if options[:profile]
              require 'ruby-prof'
              RubyProf.start
            end
            launcher.run

            while readable_io = IO.select([self_read])
              signal = readable_io.first[0].gets.strip
              handle_signal(signal)
            end
          rescue Interrupt
            logger.info 'Shutting down from sched'
            if is_master?
              scheduler.stop
            end
            release_master_lock!
            # Thread.new { release_master_lock! }
            launcher.stop
            # Explicitly exit so busy Processor threads can't block
            # process shutdown.
            exit(0)
          end
        end

        def handle_signal(sig)
          Sidekiq.logger.debug "Got #{sig} signal"
          case sig
          when 'INT'
            if Sidekiq.options[:profile]
              result = RubyProf.stop
              printer = RubyProf::GraphHtmlPrinter.new(result)
              File.open("profile.html", 'w') do |f|
                printer.print(f, :min_percent => 1)
              end
            end
            # Handle Ctrl-C in JRuby like MRI
            # http://jira.codehaus.org/browse/JRUBY-4637
            release_master_lock!
            raise Interrupt
          when 'TERM'
            # Heroku sends TERM and then waits 10 seconds for process to exit.
            release_master_lock!
            raise Interrupt
          when 'USR1'
            Sidekiq.logger.info "Received USR1, no longer accepting new work"
            release_master_lock!
            launcher.manager.async.stop
          when 'USR2'
            if Sidekiq.options[:logfile]
              Sidekiq.logger.info "Received USR2, reopening log file"
              release_master_lock!
              initialize_logger
            end
          when 'TTIN'
            release_master_lock!
            Thread.list.each do |thread|
              Sidekiq.logger.info "Thread TID-#{thread.object_id.to_s(36)} #{thread['label']}"
              if thread.backtrace
                Sidekiq.logger.info thread.backtrace.join("\n")
              else
                Sidekiq.logger.info "<no backtrace available>"
              end
            end
          end
        end
      end
    end
  end
end

Sidekiq::CLI.send(:include, SidekiqScheduler::CLI)