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
        1_000_000.times do |index|
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

    MODIFIED_FILE_MINIMUM_SIZE_IN_MB = 1*1024*1024

    ZIPFILE_MAX_SIZE_IN_MB = 1*1024*1014
    ZIPFILE_PARTS_EXT = '_part'

    def initialize (directory, zip_filename)
        @zip_filename = zip_filename
        zip_init

        @modified_files = []

        @dw = DirectoryWatcher.new directory
        @dw.interval = INTERVAL_SECS
        @dw.stable = 2 # 'stable' events triggered when file has been untouched for x2 intervals 
        @dw.add_observer {|*args| args.each {|event| event_trigger(directory, event)}}
        @dw.start
    end

    def destroy
        @zip.close
        @dw.stop
    end

    def event_trigger(directory, event)
        filename = File.basename(event.path)

        # add file to zip when 'stable' event is triggered 
        if event.type == :stable
            $logger.debug("stable file detected #{filename}, ready to zip it")
            @zip.add(filename, event.path) unless @zip.find_entry filename # don't allow duplicates
$logger.debug("Simulating zipping large file #{filename}")
sleep(1.0*20)
$logger.debug('large zipping done!')
            @zip.commit
            zip_size = File.size(@zip_filename)
            if zip_size > ZIPFILE_MAX_SIZE_IN_MB
                $logger.debug("#{@zip_filename} is getting too large (#{zip_size/1_000_000.0}MB>#{ZIPFILE_MAX_SIZE_IN_MB}MB)")
                @zip.close
                zip_send
                zip_init
            end

        #elsif event.type == :modified && event.stat.size > MODIFIED_FILE_MINIMUM_SIZE_IN_MB
        #    $logger.debug("modified file detected #{filename}, ready to zip it")
        #    @modified << { :filename: filename, 
        #                   :
        #    }
        end
    end

    private

    def zip_init
        @zip_part = @zip_part == nil ? 1 : @zip_part+1
        ext = (@zip_part == 1) ? '' : ZIPFILE_PARTS_EXT + @zip_part.to_s
        @zip_filename_current = File.basename(@zip_filename, '.zip') + ext + '.zip'
        @zip = ::Zip::File.open(@zip_filename_current, !File.exist?(@zip_filename_current))
    end

    def zip_send
    end

end

INPUT_PATH = 'bin/input'
ZIP_FILENAME = 'bin/output/small.zip'

thr = Thread.new { 
    generate_dossiers_csv(File.join(INPUT_PATH, 'dossiers.csv')) 
    puts 'Working on dossiers.csv ...'
}

ExtractionWatcher.new(INPUT_PATH, ZIP_FILENAME) 

thr.join
dw.stop
puts 'Done.'