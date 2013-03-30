#! /usr/bin/env ruby

require 'rubygems'
require 'mysql2'
require 'inifile'

PIDFILE = '/var/run/repliwatchd.pid'
LOGFILE = '/var/log/repliwatchd.log'
MAX_INTERVAL = 60

def generic_log message
  open(LOGFILE, 'a') do |f|
    f.printf("%s %s[%d]: %s\n" % [Time.now.strftime("%c"), $0, Process.pid, message.to_s])
  end
end

class RepliWatchDaemon
  def initialize
    @is_running = true
    @interval = MAX_INTERVAL
  end

  def start
    if Process.fork then exit!(0); end
    Process.setsid

    $stdin.reopen("/dev/null", "r")
    $stdout.reopen("/dev/null", "w")
    $stderr.reopen("/dev/null", "w")

    open(PIDFILE, 'w') { |f| f.puts(Process.pid) }
    at_exit { File.unlink(PIDFILE) }
    self.serve
  end

  def connect_to_database
    ini = IniFile.load("#{ENV["HOME"]}/.my.cnf")
    begin
      @dbh = Mysql2::Client.new(username: ini['client']['user'], password: ini['client']['password'])
    rescue Mysql2::Error
      sleep @interval
      retry
    end
  end

  def log message
    generic_log message
  end

  def skip_statements n
    @dbh.query('stop slave')
    @dbh.query('set global sql_slave_skip_counter = %d' % n)
    @dbh.query('start slave')
  end

  def check_sql_running slave_status
    if 'yes' != slave_status['Slave_SQL_Running'].downcase
      if [1032, 1062].include? slave_status['Last_SQL_Errno'].to_i
        skip_statements 1
        @interval = 0
      end

      log slave_status['Last_SQL_Error']
    end
  end

  def check_io_running slave_status
    if 'yes' != slave_status['Slave_IO_Running'].downcase
      log slave_status['Last_IO_Error']
      @interval = 0
    end
  end

  def serve
    $0 = 'repliwatchd'
    connect_to_database

    while @is_running
      slave_status = @dbh.query('show slave status').each { |row| row }[0]

      check_sql_running slave_status
      check_io_running slave_status

      sleep @interval = (@interval += 1) > MAX_INTERVAL ? MAX_INTERVAL : @interval
    end
  end
end

begin
  RepliWatchDaemon.new.start
rescue => e
  generic_log e
end

