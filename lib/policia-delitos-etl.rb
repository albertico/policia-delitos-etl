# encoding: utf-8

require 'rubygems'
require 'bundler/setup'
require 'activerecord-postgis-adapter'
require 'rgeo'
require 'rgeo-activerecord'
require 'rgeo-shapefile'

module Policia
  module Delitos
    module ETL
      # Transform and load shapefile into target database.
      # Shapefile geometries are reprojected from the specified coordinate system.  Columns and conversions are hardcoded.
      # The following assumptions are made:
      #  - Specified SRID must be a cartesian coordinate system.
      #  - Requires Proj4 to be installed on your system.
      #  - The expected geometry for each feature is Point (Shape Type => 1).  Any other geometry (Shape Type) will raise an exception.
      #  - Backend database must be PostgreSQL with PostGIS extension.
      #  - All features are reprojected to WGS84 and stored on a PostGIS Geography datatype column.
      #  - Shapefile must have auxiliary files available for proper parsing and attributes reading.
      #  - Expected attributes and datatypes on shapefile:
      #    - OBJECTID => Fixnum
      #    - fecha_ocur => Date
      #    - hora_ocurr => String
      #    - FK_delito_ => Fixnum
      #    - POINT_X => Float
      #    - POINT_Y => Float
      def self.transform_and_load_shapefile(shapefile, srid, force_delete=true)
        # Validate SRID not nil.
        raise "Error processing shapefile: SRID must be specified" unless srid
        # Load proj4 EPSG data file on system.
        epsg = RGeo::CoordSys::SRSDatabase::Proj4Data.new('epsg')
        # Validate that SRID exists on EPSG data file.
        raise "Error processing shapefile: Unknown SRID #{srid}" unless epsg.get(srid)
        # Create factory for specified SRID using proj4 defined on EPSG data file.
        srid_factory = RGeo::Cartesian.factory(:srid => srid, :proj4 => epsg.get(srid).proj4)
        # Create WGS84 factory.
        wgs84_factory = RGeo::Geographic.spherical_factory(:srid => 4326, :proj4 => epsg.get(4326).proj4)
        # Do ETL for each shapefile feature.
        RGeo::Shapefile::Reader.open(shapefile, :srid => srid, :factory => srid_factory) do |shp|
          # Validate shape type code and attributes.
          raise "Error processing shapefile: Shape not of Point type (1)" unless shp.shape_type_code == 1
          raise "Error processing shapefile: Attributes not available" unless shp.attributes_available?
          # Output number of records for debugging purposes.
          puts "Shapefile contains #{shp.size} records."
          # Enclose ETL process into model transaction in order to guarantee data consistency if exception is raised.
          Policia::Delitos::Model.transaction do
            if force_delete
              Policia::Delitos::Model.delete_all
              puts "Deleting ALL existing records"
            end
            # Do ETL!
            puts "Traversing shapfile features..."
            new_records = 0
            updated_records = 0
            shp.each do |feature|
              delito_attr = {}
              # :object_id -- OBJECTID
              delito_attr['object_id'] = feature.attributes['OBJECTID'].to_i
              # :delito -- FK_delito_
              delito_attr['delito'] = feature.attributes['FK_delito_'].to_i
              # :delito_description -- Use Helper module
              delito_attr['delito_description'] = Policia::Delitos::ETL::Helper.get_delito_description(delito_attr['delito'])
              # :delito_datetime -- fecha_ocur + hora_ocurr (no timezone data is stored in database)
              delito_attr['delito_datetime'] = DateTime.parse("#{feature.attributes['fecha_ocur'].iso8601}T#{feature.attributes['hora_ocurr']}")
              # :delito_date, :delito_time, :delito_year, :delito_month, :delito_day
              delito_attr['delito_date'] = delito_attr['delito_datetime'].to_date
              delito_attr['delito_time'] = delito_attr['delito_datetime'].to_time
              delito_attr['delito_year'] = delito_attr['delito_datetime'].to_date.year
              delito_attr['delito_month'] = delito_attr['delito_datetime'].to_date.month
              delito_attr['delito_day'] = delito_attr['delito_datetime'].to_date.day
              # :geom -- reprojected point (WGS84)
              delito_attr['geom'] = RGeo::Feature.cast(feature.geometry, :factory => wgs84_factory, :project => true)
              # Create or find existing record
              record = (force_delete ? nil : Policia::Delitos::Model.find_by(object_id: delito_attr['object_id'])) || Policia::Delitos::Model.new
              record.attributes = delito_attr
              # Output ID, SRID and geometries for debugging purposes.
              puts "[#{record.new_record? ? "N" : "U"}] [#{record.object_id}] SRID:#{feature.geometry.srid} #{feature.geometry} => SRID:#{record.geom.srid} #{record.geom}"
              # Increase counters.
              record.new_record? ? new_records += 1 : updated_records += 1
              # Save record!
              record.save
            end
            puts "TOTAL: #{shp.size}  [N] => #{new_records}  [U] => #{updated_records}"
          end
        end
      end
      
      module Helper
        def self.get_delito_description(delito)
          case delito
            when 1 then "Asesinato"
            when 2 then "Violación"
            when 3 then "Robo"
            when 4 then "Agresión Agravada"
            when 5 then "Escalamiento"
            when 6 then "Apropiación Ilegal"
            when 7 then "Vehículo Hurtado"
            when 8 then "Incendio Malicioso"
            else ""
          end
        end
      end
    end

    class Schema < ActiveRecord::Migration
      def create(drop_if_exists=false)
        drop if drop_if_exists
        create_table :policia_delitos, :id => false do |t|
          # Columns
          t.integer :object_id
          t.integer :delito
          t.string :delito_description
          t.datetime :delito_datetime
          t.date :delito_date
          t.time :delito_time
          t.integer :delito_year
          t.integer :delito_month
          t.integer :delito_day
          t.point :geom, :geographic => true # By default, PostGIS Geography datatypes have SRID 4326 (WGS84).
          # Indexes
          t.index :object_id, :unique => true
          t.index :geom, :spatial => true # Spatial index.
        end
        # Workaround to define custom primary key.
        execute "ALTER TABLE #{:policia_delitos} ADD PRIMARY KEY (#{:object_id});"
      end

      def drop
        drop_table :policia_delitos if exists?
      end

      def exists?
        table_exists? :policia_delitos
      end
    end

    class Model < ActiveRecord::Base
      self.table_name = :policia_delitos
      self.primary_key = :object_id
    end
  end
end
