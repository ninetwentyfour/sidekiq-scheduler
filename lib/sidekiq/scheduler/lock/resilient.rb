require 'sidekiq/scheduler/lock/base'

module Sidekiq
  class Scheduler
    module Lock
      class Resilient < Base
        def acquire!
          Sidekiq.redis { |r| r.evalsha(
            acquire_sha,
            :keys => [key],
            :argv => [value]
          )}.to_i == 1
          # Sidekiq.redis.evalsha(
          #   acquire_sha,
          #   :keys => [key],
          #   :argv => [value]
          # ).to_i == 1
        end

        def locked?
          Sidekiq.redis { |r| r.evalsha(
            locked_sha,
            :keys => [key],
            :argv => [value]
          )}.to_i == 1
          # Sidekiq.redis.evalsha(
          #   locked_sha,
          #   :keys => [key],
          #   :argv => [value]
          # ).to_i == 1
        end

      private

        def locked_sha(refresh = false)
          @locked_sha = nil if refresh

          @locked_sha ||= begin
            Sidekiq.redis { |r| r.script(
              :load,
              <<-EOF
if redis.call('GET', KEYS[1]) == ARGV[1]
then
  redis.call('EXPIRE', KEYS[1], #{timeout})

  if redis.call('GET', KEYS[1]) == ARGV[1]
  then
    return 1
  end
end

return 0
EOF
            )}
#             Sidekiq.redis.script(
#               :load,
#               <<-EOF
# if redis.call('GET', KEYS[1]) == ARGV[1]
# then
#   redis.call('EXPIRE', KEYS[1], #{timeout})

#   if redis.call('GET', KEYS[1]) == ARGV[1]
#   then
#     return 1
#   end
# end

# return 0
# EOF
#             )
          end
        end

        def acquire_sha(refresh = false)
          @acquire_sha = nil if refresh

          @acquire_sha ||= begin
            Sidekiq.redis { |r| r.script(
              :load,
              <<-EOF
if redis.call('SETNX', KEYS[1], ARGV[1]) == 1
then
  redis.call('EXPIRE', KEYS[1], #{timeout})
  return 1
else
  return 0
end
EOF
            )}
#             Sidekiq.redis.script(
#               :load,
#               <<-EOF
# if redis.call('SETNX', KEYS[1], ARGV[1]) == 1
# then
#   redis.call('EXPIRE', KEYS[1], #{timeout})
#   return 1
# else
#   return 0
# end
# EOF
#             )
          end
        end
      end
    end
  end
end
