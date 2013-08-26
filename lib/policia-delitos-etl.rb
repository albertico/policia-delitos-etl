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
      def self.transform_and_load_shapefile(shapefile, srid)
        raise "Error processing shapefile: SRID must be specified" unless srid
        # Load proj4 EPSG data file on system.
        epsg = RGeo::CoordSys::SRSDatabase::Proj4Data.new('epsg')
        # Create factory for specified SRID using proj4 defined on EPSG data file.
        raise "Error processing shapefile: Unknown SRID #{srid}" unless epsg.get(srid)
        srid_factory = RGeo::Cartesian.factory(:srid => srid, :proj4 => epsg.get(srid).proj4)
        # Create WGS84 factory.
        wgs84_factory = RGeo::Geographic.spherical_factory(:srid => 4326, :proj4 => epsg.get(4326).proj4)
        # Do ETL for each shapefile feature.
        RGeo::Shapefile::Reader.open(shapefile, :srid => srid, :factory => srid_factory) do |shp|
          raise "Error processing shapefile: Shape not of Point type (1)" unless shp.shape_type_code == 1
          raise "Error processing shapefile: Attributes not available" unless shp.attributes_available?
          puts "File '#{shapefile}' contains #{shp.size} records."
          Policia::Delitos::Model.transaction do
            shp.each do |feature|
              delito_incidente = Policia::Delitos::Model.new do |record|
                # :object_id -- OBJECTID
                record.object_id = feature.attributes['OBJECTID'].to_i
                # :delito -- FK_delito_
                delito = feature.attributes['FK_delito_'].to_i
                record.delito = delito
                # :delito_description -- Use Helper module
                record.delito_description = Policia::Delitos::ETL::Helper.get_delito_description(delito)
                # :delito_datetime -- fecha_ocur + hora_ocurr (no timezone data is stored on database)
                feature_datetime = DateTime.parse("#{feature.attributes['fecha_ocur'].iso8601}T#{feature.attributes['hora_ocurr']}")
                record.delito_datetime = feature_datetime
                # :delito_year, :delito_month, :delito_day, :delito_time
                record.delito_year = feature_datetime.to_date.year
                record.delito_month = feature_datetime.to_date.month
                record.delito_day = feature_datetime.to_date.day
                record.delito_time = feature_datetime.to_time
                # :geom -- reprojected point (WGS84)
                record.geom = RGeo::Feature.cast(feature.geometry, :factory => wgs84_factory, :project => true)
                # Output ID and geometries for debugging purposes.
                puts "[#{record.object_id}] SRID:#{srid} #{feature.geometry} => #{record.geom}"
              end
              delito_incidente.save
            end
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
          t.integer :delito_year
          t.integer :delito_month
          t.integer :delito_day
          t.time :delito_time
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
