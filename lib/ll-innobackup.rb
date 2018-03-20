require 'json'
require 'date'

# Run Inmental or Full backups on MySQL
module LL
class InnoBackup
  class << self
    # Use this in case the log file is massive
    def options
      JSON.parse(File.read('/etc/mysql/innobackupex.json'))
    rescue Errno::ENOENT
      {}
    end

    def tail_file(path, n)
      file = File.open(path, 'r')
      buffer_s = 512
      line_count = 0
      file.seek(0, IO::SEEK_END)

      offset = file.pos # we start at the end

      while line_count <= n && offset > 0
        to_read = if (offset - buffer_s) < 0
                    offset
                  else
                    buffer_s
                  end

        file.seek(offset - to_read)
        data = file.read(to_read)

        data.reverse.each_char do |c|
          if line_count > n
            offset += 1
            break
          end
          offset -= 1
          line_count += 1 if c == "\n|"
        end
      end

      file.seek(offset)
      file.read
    end

    def state_file(t)
      "/tmp/backup_#{t}_state"
    end

    def lock_file(type)
      "/tmp/backup_#{type}.lock"
    end

    def innobackup_log(t)
      "/tmp/backup_#{t}_innobackup_log"
    end
  end

  attr_reader :type,
              :now,
              :date,
              :state_files,
              :lock_files,
              :options

  def initialize(options = {})
    @now = Time.now
    @date = @now.to_date
    @options = options
    @lock_files = {}
    @state_files = {}
    @type = backup_type
  end

  def aws_log
    "/tmp/backup_#{type}_aws_log"
  end

  def innobackup_log
    "/tmp/backup_#{type}_innobackup_log"
  end

  def lock?(t = type)
    lock_files[t] ||= File.new(InnoBackup.lock_file(t), File::CREAT)
    lock_files[t].flock(File::LOCK_NB | File::LOCK_EX).zero?
  end

  def state(t)
    state_files[t] ||= JSON.parse(File.read(InnoBackup.state_file(t)))
  rescue JSON::ParserError
    puts 'unable to stat state file'
    {}
  end

  def fully_backed_up_today?
    require 'active_support/all'
    date = state('full')['date']
    Time.parse(date).today?
  rescue Errno::ENOENT
    puts 'unable to obtain last full backup state'
    false
  rescue NoMethodError
    puts 'unable to obtain last backup state'
    false
  end

  def can_full_backup?
    !fully_backed_up_today? && lock?('full')
  end

  def full_backup_running?
    !lock?('full')
  end

  def incremental_backup_running?
    !lock?('incremental')
  end

  def backup_type
    return 'full' unless fully_backed_up_today? || full_backup_running?
    return 'incremental' unless incremental_backup_running?
    raise 'Unable to backup as backups are running'
  end

  def backup_bin
    @backup_bin = options['backup_bin'] ||= '/usr/bin/innobackupex'
  end

  def backup_parallel
    @backup_parallel = options['backup_parallel'] ||= 4
  end

  def backup_compress_threads
    @backup_compress_threads = options['backup_compress_threads'] ||= 4
  end

  def sql_backup_user
    @sql_backup_user ||= options['sql_backup_user']
  end

  def sql_backup_password
    @sql_backup_password ||= options['sql_backup_password']
  end

  def aws_bin
    @aws_bin = options['aws_bin'] ||= '/usr/local/bin/aws'
  end

  def aws_bucket
    raise NoStateError, 'aws_bucket not provided' unless options['aws_bucket']
    @aws_bucket = options['aws_bucket']
  end

  def expected_full_size
    @expected_full_size = options['expected_full_size'] ||= 1_600_000_000
  end

  def sql_authentication
    "--user=#{sql_backup_user} --password=#{sql_backup_password}"
  end

  def innobackup_options
    "--parallel=#{backup_parallel} "\
    "--compress-threads=#{backup_compress_threads} "\
    '--stream=xbstream --compress'
  end

  def innobackup_command
    "#{backup_bin} #{sql_authentication} "\
    "#{incremental} #{innobackup_options} /tmp/sql"
  end

  def expires_date
    require 'active_support/all'
    # Keep incrementals for 2 days
    return (@now + 2.days).iso8601 if type == 'incremental'
    # Keep first backup of month for 180 days
    return (@now + 6.months).iso8601 if @date.yesterday.month != @date.month
    # Keep first backup of week for 31 days (monday)
    return (@now + 1.month).iso8601 if @date.cwday == 1
    # Keep daily backups for 14 days
    (@now + 2.weeks).iso8601
  end

  def expires
    ed = expires_date
    "--expires=#{ed}" if ed
  end

  def expected_size
    "--expected-size=#{expected_full_size}" if type == 'full'
  end

  def aws_command
    "#{aws_bin} s3 cp - s3://#{aws_bucket}/#{aws_backup_file}"\
    " #{expected_size} #{expires}"
  end

  def valid_commands?
    File.exist?(backup_bin) && File.exist?(aws_bin)
  end

  def backup
    require 'English'
    exc = "#{innobackup_command} 2> #{innobackup_log} |
    #{aws_command} 2> #{aws_log} >> #{aws_log}"
    `#{exc}`if valid_commands?
    @completed = $CHILD_STATUS == 0
    return record if success? && completed?
    revert_aws if valid_commands?
  rescue InnoBackup::NoStateError => e
    STDERR.puts e.message
  ensure
    report
  end

  def revert_aws
    exc = "#{aws_bin} s3 rm s3://#{aws_bucket}/#{aws_backup_file} > /dev/null 2>/dev/null"
    `#{exc}`
  end

  def success?
    InnoBackup.tail_file(
      innobackup_log,
      1
    ) =~ /: completed OK/
  rescue Errno::ENOENT
    false
  end

  def record
    File.write(
      InnoBackup.state_file(type),
      {
        date: now,
        lsn: lsn_from_backup_log,
        file: aws_backup_file
      }.to_json
    )
  end

  def incremental
    return unless backup_type == 'incremental'
    "--incremental --incremental-lsn=#{lsn_from_state}"
  end

  def lsn_from_full_backup_state?
    Time.parse(state('full')['date']) > Time.parse(state('incremental')['date'])
  rescue Errno::ENOENT
    true
  end

  def lsn_from_state
    return state('full')['lsn'] if lsn_from_full_backup_state?
    state('incremental')['lsn']
  rescue NoMethodError
    raise NoStateError, 'no state file for incremental backup'
  end

  def lsn_from_backup_log
    matches = InnoBackup.tail_file(
      InnoBackup.innobackup_log(type),
      30
    ).match(/The latest check point \(for incremental\): '(\d+)'/)
    matches[1] if matches
  end

  def hostname
    return options[:hostname] if options[:hostname]
    require 'socket'
    Socket.gethostbyname(Socket.gethostname).first
  end

  def aws_backup_file
    return "#{hostname}/#{now.iso8601}/percona_full_backup" if type == 'full'
    "#{hostname}/#{Time.parse(state('full')['date']).iso8601}/percona_incremental_#{now.iso8601}"
  rescue NoMethodError
    raise NoStateError, 'incremental state missing or corrupt'
  end

  def completed?
    @completed == true
  end

  def report
    # Eventually Tell Zabbix
    if success? && completed?
      STDERR.puts "#{$PROGRAM_NAME}: success: completed #{type} backup"
      return
    end
    STDERR.puts "#{$PROGRAM_NAME}: failed" 
    STDERR.puts 'missing binaries' unless valid_commands?
    inno_tail = InnoBackup.tail_file(innobackup_log, 10)
    STDERR.puts 'invalid sql user' if inno_tail =~ /Option user requires an argument/
    STDERR.puts 'unable to connect to DB' if inno_tail =~ /Access denied for user/
    STDERR.puts 'insufficient file access' if inno_tail =~ /Can't change dir to/
    aws_tail = InnoBackup.tail_file(aws_log, 10)
    STDERR.puts 'bucket incorrect' if aws_tail =~ /The specified bucket does not exist/
    STDERR.puts 'invalid AWS key' if aws_tail =~ /The AWS Access Key Id you/
    STDERR.puts 'invalid Secret key' if aws_tail =~ /The request signature we calculated/
  end

  class NoStateError < StandardError
  end
end
end
InnoBackup.new(InnoBackup.options).backup if $PROGRAM_NAME == __FILE__