# frozen_string_literal: true

require 'csv'
require 'date'
require 'directory_watcher'
require 'logger'
require 'zip'

class SingletonLogger

  @@instance = Logger.new($stdout)
  def self.instance
    @@instance
  end
  private_class_method(:new)
end

class ExtractionWatcher

  def initialize(input_dir, output_dir, zip_filename, watch_interval, chuck_size, output_prefix)
    @output_dir = output_dir
    @zip_filename = zip_filename
    @chuck_size = chuck_size

    @stable_files = StableFiles.new(output_dir, zip_filename, chuck_size, output_prefix)
    @large_files = LargeFiles.new(output_dir, chuck_size, output_prefix)

    @dw = DirectoryWatcher.new input_dir
    @dw.interval = watch_interval
    @dw.stable = 2 # 'stable' events triggered when file has been untouched for x2 intervals
    @dw.add_observer { |*args| args.each { |event| event_trigger(event) } }
    @dw.start
  end

  def destroy
    @dw.stop
  end

  def flush
    @stable_files.close_zip
    @large_files.flush_leftover_to_zip
  end

  def event_trigger(event)
    if event.type == :stable
      SingletonLogger.instance.debug("stable #{event}")
      @stable_files.add(event.path)
    elsif event.type == :modified && event.stat.size > @chuck_size
      SingletonLogger.instance.debug("modified #{event}")
      @large_files.handle_modify(event.path)
    end
  end
end

class StableFiles
  
  def initialize(output_dir, zip_filename, chuck_size, output_prefix)
    @output_dir = output_dir
    @zip_filename = zip_filename
    @chuck_size = chuck_size
    @output_prefix = output_prefix

    @zip_fullfilename = File.join(@output_dir, @zip_filename)
    zip_init

    @counter = 1
    @dict = {}
  end

  def close_zip
    @zip.commit
    @zip.close
  end

  def add(event_path)
    zip_init if @create_new_zip
    filename = File.basename(event_path)
    SingletonLogger.instance.debug("stable files - adding #{@counter} #{filename}")

    # detects duplicates and change name if necessary
    if @dict[filename]
      current_index = @dict[filename]
      current_index += 1
      @dict[filename] = current_index
      filename = "#{File.basename(filename, '.*')}_duplicate_#{current_index}#{File.extname(filename)}"
      SingletonLogger.instance.debug("stable files - duplicate added file #{filename}")
    else
      @dict[filename] = 1
    end
    @zip.add(filename, event_path)

    @counter += 1
    return unless (@counter % 100).zero?

    @zip.commit
    zip_size = File.size(@zip_filename_current)
    SingletonLogger.instance.debug("stable files - commit, checking zip size : #{zip_size / 1_000_000.0}MB")

    return unless zip_size > @chuck_size

    @create_new_zip = true
    zip_size_mb = "#{zip_size / 1_000_000.0}MB"
    SingletonLogger.instance.debug("stable files - closing zip at #{zip_size_mb} > #{@chuck_size}MB")
    @zip.close
    # zip_send_threaded
  end

  private

  def zip_init
    @create_new_zip = false
    @zip_part = @zip_part.nil? ? 1 : @zip_part + 1
    ext = @zip_part == 1 ? '' : "_part#{@zip_part}"
    @zip_filename_current = File.join(@output_dir,
                                      "#{@output_prefix}#{File.basename(@zip_filename, '.zip')}#{ext}.zip")
    @zip = ::Zip::File.open(@zip_filename_current, !File.exist?(@zip_filename_current))
    SingletonLogger.instance.debug "stable files - creates new zip #{@zip_filename_current}"
  end

  # def zip_send_threaded
  # end
end

class LargeFiles

  def initialize(output_dir, batch_size, output_prefix)
    @output_dir = output_dir
    @batch_size = batch_size
    @output_prefix = output_prefix

    @large_files = []
  end

  def handle_modify(full_filename)
    # SingletonLogger.instance.debug("large files - large file being watched : #{full_filename}")
    retrieve_large_file(@output_dir, full_filename).zip_new_batch_of_bytes @batch_size
  end

  def flush_leftover_to_zip
    SingletonLogger.instance.debug('flushing leftover for all large files ...') if @large_files.count.positive?
    @large_files.each(&:flush_leftover_to_zip)
  end

  private

  def retrieve_large_file(output_dir, full_filename)
    @large_files.each do |large_file|
      return large_file if large_file.full_filename == full_filename
    end
    SingletonLogger.instance.debug('large files - new large file object created')
    large_file = LargeFile.new(output_dir, full_filename, @output_prefix)
    @large_files << large_file
    large_file
  end
end

class LargeFile

  attr_reader :full_filename

  def initialize(output_dir, full_filename, output_prefix)
    @output_dir = output_dir
    @full_filename = full_filename
    @output_prefix = output_prefix

    @pos = 0
    @part = 0
    zip_full_filename = File.join(output_dir, "#{@output_prefix}#{File.basename(@full_filename, '.*')}.zip")
    @zip = ::Zip::File.open(zip_full_filename, true)
    SingletonLogger.instance.debug("large file - creates a new zip file #{zip_full_filename}")
  end

  def flush_leftover_to_zip
    SingletonLogger.instance.debug("flushing leftover to zip for #{@full_filename}")
    zip_new_batch_of_bytes(1)
    @zip.close
  end

  def zip_new_batch_of_bytes(batch_size)
    # get size and wait until file have increased enough
    size = File.size(@full_filename)
    return unless size / batch_size > @part

    length = size - @pos
    data = IO.read(@full_filename, length, @pos)
    # SingletonLogger.instance.debug("large file - debug read info : pos=#{@pos} length=#{length}")

    # add to zip & make sure everything was flushed on the disk
    file_size_mb = "size in MB: #{size / 1_000_000.0}"
    data_mb = "data added in MB:#{data.size / 1_000_000.0}"
    SingletonLogger.instance.debug("large file - adding #{archive_filename} to zip (#{file_size_mb} / #{data_mb}")
    @zip.get_output_stream(archive_filename) { |f| f.puts data }
    @zip.commit

    # get ready for the next batch
    @pos = size
    @part += 1
  end

  private

  def archive_filename
    File.basename(@full_filename, '.*') + (@part.zero? ? '' : "_part#{@part + 1}") + File.extname(@full_filename)
  end
end

output_prefix = "#{DateTime.now.strftime('%s')}_"

def command_line_argument(name)
  index = ARGV.index name.to_s
  ARGV[index + 1] if index && ARGV.length >= index
end

input_dir = command_line_argument '--input_dir'
output_dir = command_line_argument '--output_dir'
stables_zip_name = command_line_argument '--stables_zip_name'
watch_interval = command_line_argument '--watch_interval'
chuck_size = command_line_argument '--chuck_size'

input_dir = !input_dir ? '/tmp/input' : input_dir
output_dir = !output_dir ? '/tmp/output' : output_dir
stables_zip_name = !stables_zip_name ? 'stables.zip' : stables_zip_name
watch_interval = !watch_interval ? 1 : watch_interval
chuck_size = !chuck_size ? 20 * 1024 * 1024 : chuck_size

thr = Thread.new {
  #puts 'Simulating dossiers.csv generation ...'
  #generate_dossiers_csv(File.join('bin/input', 'dossiers.csv'))
  #puts 'Finished with dossiers.csv'
  puts "Watcher started. Press [Enter] when done.\n"
  $stdin.gets
  puts 'Watcher stopped'
}

ew = ExtractionWatcher.new(input_dir, output_dir, stables_zip_name,
                           watch_interval, chuck_size, output_prefix)
thr.join
ew.flush

SingletonLogger.instance.debug('Done.')
