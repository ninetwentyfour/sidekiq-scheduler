%w[base basic resilient].each do |file|
  require "sidekiq/scheduler/lock/#{file}"
end
