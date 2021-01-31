require 'csv'
require 'logger'
require 'zip'
require 'bundler/inline'

gemfile do
  source 'https://rubygems.org'
  gem 'directory_watcher'
  gem 'faker'
  gem 'google-api-client'
end
Faker::Config.random = Random.new(42)

$logger = Logger.new(STDOUT)

def generate_dossiers_csv(filename)
    $logger.debug("Dossiers file will be : #{filename}")
    CSV.open(filename, 'wb') do |csv|
        20_000.times do |index|
            csv << [ Faker::Name.name, 
                Faker::Internet.email, 
                Faker::Company.bs, 
                Faker::CryptoCoin.coin_hash, 
                Faker::GreekPhilosophers.quote
            ]
            sleep(1.0/10000)
            $logger.debug("generating csv ... #{index} lines") if index % 10_000 == 0
        end
    end
end

class ExtractionWatcher

    INTERVAL_SECS = 2.0
    MODIFIED_FILE_MINIMUM_SIZE_IN_MB = 2*1024*1024

    def initialize(input_dir, output_dir, zip_filename)
        @output_dir = output_dir
        @zip_filename = zip_filename

        @stables_files = StableFiles.new(output_dir, zip_filename)

        @dw = DirectoryWatcher.new input_dir
        @dw.interval = INTERVAL_SECS
        @dw.stable = 2 # 'stable' events triggered when file has been untouched for x2 intervals 
        @dw.add_observer {|*args| args.each {|event| event_trigger(input_dir, event)}}
        @dw.start
    end

    def destroy
        @stables_files.close_zip
        @dw.stop
    end

    def event_trigger(directory, event)
        if event.type == :stable
            @stables_files.add(event.path)
        elsif event.type == :modified && event.stat.size > MODIFIED_FILE_MINIMUM_SIZE_IN_MB
            LargeFiles.handle_modify(@output_dir, event.path, MODIFIED_FILE_MINIMUM_SIZE_IN_MB)
        end
    end

    private

    def zip_send_threaded
    end
end

class LargeFile

    attr_reader :full_filename

    def initialize(output_dir, full_filename)
        @output_dir = output_dir
        @full_filename = full_filename
        @pos = 0
        @part = 0
        zip_full_filename = File.join(output_dir, File.basename(@full_filename, '.*') + '.zip')
        @zip = ::Zip::File.open(zip_full_filename, true)
        $logger.debug("large file - creates a new zip file #{zip_full_filename}")
    end

    def flush
        $logger.debug("flushing file #{@full_filename}")
        zip_new_batch_of_bytes(1)
        @zip.close
    end

    def zip_new_batch_of_bytes(batch_size)
        # get size and wait until file have increased enough
        size =  File.size(@full_filename)
        return unless size/batch_size > @part

        length = size-@pos
        data = IO.read(@full_filename, length, @pos)
        # $logger.debug("large file - debug read info : pos=#{@pos} length=#{length}")

        # add to zip & make sure everything was flushed on the disk
        $logger.debug("large file - adding #{archive_filename} to zip (size in MB: #{size/1_000_000.0} / data added in MB:#{data.size/1_000_000.0}")
        @zip.get_output_stream(archive_filename) { |f| f.puts data }
        @zip.commit

        # get ready for the next batch
        @pos = size
        @part += 1
    end

    private

    def archive_filename
        File.basename(@full_filename, '.*') + (@part == 0 ? '' : "_part#{@part+1}") + File.extname(@full_filename)
    end
end

class StableFiles

    ZIPFILE_PARTS_EXT = '_part'
    ZIPFILE_MAX_SIZE_IN_MB = 1*1024*1014

    def initialize(output_dir, zip_filename)
        @output_dir = output_dir
        @zip_filename = zip_filename
        @zip_fullfilename = File.join(@output_dir, @zip_filename)
        zip_init
    end
    
    def close_zip
        @zip.close
    end
    
    def add event_path
        filename = File.basename(event_path)
        $logger.debug("stable file detected #{filename}, ready to zip it to #{@zip_fullfilename}")
        @zip.add(filename, event_path) unless @zip.find_entry filename # don't allow duplicates
        @zip.commit
        zip_size = File.size(@zip_fullfilename)
        if zip_size > ZIPFILE_MAX_SIZE_IN_MB
            $logger.debug("#{@zip_fullfilename} is getting too large (#{zip_size/1_000_000}MB>#{ZIPFILE_MAX_SIZE_IN_MB}MB)")
            @zip.close
            zip_send_threaded
            @stables_files.zip_init
        end
    end

    private 

    def zip_init
        @zip_part = @zip_part == nil ? 1 : @zip_part+1
        ext = (@zip_part == 1) ? '' : ZIPFILE_PARTS_EXT + @zip_part.to_s
        @zip_filename_current = File.join(@output_dir, File.basename(@zip_filename, '.zip') + ext + '.zip')
        @zip = ::Zip::File.open(@zip_filename_current, !File.exist?(@zip_filename_current))
    end
end

class LargeFiles
    @@large_files = []

    def self.handle_modify(output_dir, full_filename, batch_size)
        # $logger.debug("large files - large file being watched : #{full_filename}")
        self.retrieve_large_file(output_dir, full_filename).zip_new_batch_of_bytes batch_size
    end

    def self.flush
        $logger.debug('flushing large files ...')
        @@large_files.each { |large_file| large_file.flush }
    end

    private

    def self.retrieve_large_file(output_dir, full_filename)
        @@large_files.each do |large_file|
            return large_file if large_file.full_filename == full_filename
        end
        $logger.debug("large files - new large file object created")
        large_file = LargeFile.new(output_dir, full_filename)
        @@large_files << large_file
        large_file
    end
end


thr = Thread.new { 
    puts 'Working on dossiers.csv ...'
    generate_dossiers_csv(File.join('bin/input', 'dossiers.csv')) 
    puts 'Done.'
}

ExtractionWatcher.new('bin/input', 'bin/output', 'small.zip') 

thr.join

LargeFiles.flush
