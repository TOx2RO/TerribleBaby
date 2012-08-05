#!/usr/bin/ruby
#coding:utf-8

require 'thread'
require 'uri'
require 'net/http'
require 'digest/md5'
require 'pp'

require 'log4r'
include Log4r

require 'redis'

# daemonize
Process.daemon(true)

# parameters
USER_AGENT = "Mozilla/4.0 (compatible; MSIE 6.0; Windows XP)"
ACCEPT_LANGUAGE = "ja"
TIMEOUT = 20
MIN_INTERVAL = 15
MAX_THREADS = 5
REDIS_QUEUE_KEY = "TB_download_queue"
REDIS_LIMITER_KEY_PREFIX = "TB_limiter_"

# global variables
is_terminating = false

# preparing logger
formatter = Log4r::PatternFormatter.new(
  :pattern => "%d %C[%l]: %M",
  :date_format => "%Y/%m/%d %H:%M:%S"
)
@logger = Log4r::Logger.new('download')
@logger.trace = true
@logger.level = INFO
outputter = Log4r::FileOutputter.new(
  "file",
  :filename => '/var/log/TB_downloader.log',
  :trunc => false,
  :formatter => formatter
)
@logger.add(outputter)

# regist signal handlers
Signal.trap(:SIGTERM) do
  @logger.info "SIGTERM received"
  is_terminating = true
end

# download function
def download(param, url, output, isRedirect)
  Net::HTTP.start(url.host, url.port) do |http|
    http.read_timeout = TIMEOUT
    response = http.get(url.path, {
        'User-Agent'      => USER_AGENT,
        'Accept-Language' => ACCEPT_LANGUAGE
      })
    
    if response.code == '200' then
      # download success
      begin
        open(output, "wb") do |f|
          f.puts response.body
        end
        @logger.info "(#{param}) downloaded #{url.to_s}"
      rescue
        @logger.info "(#{param}) invalid output file #{url.to_s}"
      end
    elsif isRedirect == true && (response.code == '301' || response.code == '302' || response.code == '303' || response.code == '307') then
      # redirected
      url2 = URI.parse(response["location"])
      @logger.info "(#{param}) #{url.to_s} is moved to #{url2.to_s}"
      download(param, url2, output, false)
    else
      @logger.info "(#{param}) error #{url.to_s} response code = #{response.code}"
    end    
  end
end

# main
@logger.info "TerribleBaby started #{$$}"
@threads = Array.new(MAX_THREADS)

MAX_THREADS.times do |i|
  @threads[i] = Thread.new(i) do |param|    
    # initialize Redis object
    redis = Redis.new
    @logger.info "(#{param}) Redis object is initialized"
    
    # go loop
    loop do
      # exit thread when SIGTERM received
      if is_terminating == true then
        @logger.info "(#{param}) thread #{param} is being terminated"
        break
      end
      
      # fetch a download entry
      tmp = redis.LPOP(REDIS_QUEUE_KEY)
      if tmp.nil? then
        sleep 3
        next
      end
 
      entry = Marshal.load(tmp)
      url = entry['url']
      output = entry['output']
      nowait = entry['nowait']
      uid = entry['uid']

      if nowait == true
        # download (nowait mode)
        @logger.info "(#{param}) start download (nowait): #{url.to_s}"
        download(param, url, output, true)
      else
        # skip when limiter is set
        if redis.SETNX(REDIS_LIMITER_KEY_PREFIX + uid, 1) == 0 then
          redis.RPUSH(REDIS_QUEUE_KEY, tmp)
          @logger.info "(#{param}) #{uid} is limitted!"
          next
        end
        
        @logger.info "(#{param}) start download: #{url.to_s}"
        download(param, url, output, true)      
      
        # make limiter to be expired in MIN_INTERVAL
        redis.EXPIRE(REDIS_LIMITER_KEY_PREFIX + uid, MIN_INTERVAL)
      end
    end
  end
end

# join the threads
MAX_THREADS.times { |i|
  @threads[i].join
}
@logger.info "TerribleBaby terminated #{$$}"