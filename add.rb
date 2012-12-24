#!/usr/bin/ruby
#coding:utf-8

require 'rubygems'

require 'thread'
require 'uri'
require 'net/http'
require 'digest/md5'
require 'pp'

require 'redis'

# parameters
REDIS_QUEUE_KEY = "TB_download_queue"

# get the arguments
abort 'add.rb [URL] [output file/dir] (--nowait) (--uid uniqueID)' unless ARGV.count >= 2
arg_url = ARGV.shift
arg_output = ARGV.shift
arg_nowait = false
arg_uid = nil
while arg = ARGV.shift
  case arg
    when "--nowait"
      arg_nowait = true
    when "--uid"
      arg_uid = ARGV.shift
  end
end

# check arguments
url = nil
begin
  url = URI.parse(arg_url)
  raise URI::InvalidURIError if url.scheme == nil
rescue URI::InvalidURIError
  abort 'bad URL'
end
arg_uid = url.host if arg_uid == nil

dir = nil
filename = nil
arg_output = File.expand_path(arg_output)
if FileTest.directory?(arg_output) then
  dir = arg_output
else
  dir = File.dirname(arg_output)
  abort "no such output directory" unless FileTest.directory?(dir)
  filename = File.basename(arg_output)
end

filename = url.path.split('/').pop if filename == nil

# add a record
value = Hash.new
value['url'] = url
value['output'] = "#{dir}/#{filename}"
value['nowait'] = arg_nowait
value['uid'] = arg_uid

# put an entry to Redis
redis = Redis.new
redis.rpush(REDIS_QUEUE_KEY, Marshal.dump(value))

if value['nowait'] then
  puts "success (nowait)"
else
  puts "success"
end
