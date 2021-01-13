Gem::Specification.new do |s|
  s.name        = 'll-innobackup'
  s.version     = '0.1.23'
  s.summary     = "Livelink Innobackup Script"
  s.description = "A program to conduct innobackup"
  s.authors     = ["Stuart Harland, LiveLink Technology Ltd"]
  s.email       = 'essjayhch@gmail.com, infra@livelinktechnology.net'
  s.files       = ["lib/ll-innobackup.rb"]
  s.homepage    =
    'http://rubygems.org/gems/ll-innobackup'
  s.license       = 'MIT'
  s.executables << 'll-innobackup'
  s.add_dependency 'activesupport', '= 4.2.6'
  s.add_dependency 'aws-sdk-s3', '~> 1'
end
