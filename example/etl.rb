#!/usr/bin/env ruby

require '../lib/policia-delitos-etl'

ActiveRecord::Base.configurations = YAML::load(IO.read('database.yml'))
ActiveRecord::Base.establish_connection('development')
policia_delitos_schema = Policia::Delitos::Schema.new
policia_delitos_schema.drop
policia_delitos_schema.create
Policia::Delitos::ETL.transform_and_load_shapefile('delitos.shp', 2866)
