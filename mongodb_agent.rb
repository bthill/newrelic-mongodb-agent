#!/usr/bin/env ruby
require 'rubygems'
require 'bundler/setup'
require 'newrelic_plugin'
require 'mongo'

include Mongo

module NewRelic::MongodbAgent

  class Agent < NewRelic::Plugin::Agent::Base
    agent_guid 'com.naspersclassifieds.mongo-agent'
    agent_config_options :endpoint, :username, :password, :port, :agent_name, :ssl
    agent_human_labels('MongoDB') { "#{agent_name}" }
    agent_version '2.5.0-naspersclassifieds'

    def setup_metrics
      self.port ||= 27017
      self.agent_name ||= "#{endpoint}:#{port}/#{database}"
    end

    def poll_cycle
      stats = mongodb_server_stats()

      # Network metrics
      report_counter_metric('Network/Bytes In', 'bytes/sec', stats['network']['bytesIn'])
      report_counter_metric('Network/Bytes Out', 'bytes/sec', stats['network']['bytesOut'])
      report_counter_metric('Network/Requests', 'requests/sec', stats['network']['numRequests'])

      # Ops counters
      report_section('Opcounters', stats['opcounters'])

      # Ops counters replication
      report_section('Opcounters Replication', stats['opcountersRepl'])

      # Faults and assertions
      report_counter_metric('Extra/Page Faults', 'pagefaults/sec', stats['extra_info']['page_faults'])
      report_section('Asserts', stats['asserts'])

      # Connections
      report_metric('Connections/Current', 'current', stats['connections']['current'])
      report_metric('Connections/Available', 'available', stats['connections']['available'])
      report_counter_metric('Connections/Total Created', 'connections/sec', stats['connections']['totalCreated'])

      # Cursor metrics
      report_metric('Cursors/Open Total', 'open', stats['metrics']['cursor']['open']['total'])
      report_metric('Cursors/Open No Timeout', 'open', stats['metrics']['cursor']['open']['noTimeout'])
      report_metric('Cursors/Open Pinned', 'open', stats['metrics']['cursor']['open']['pinned'])
      report_metric('Cursors/Timed Out', 'timedout', stats['metrics']['cursor']['timedOut'])

      # Memory metrics retrieved in MB from MongoDB, converted to bytes for New Relic graphing
      ## Main
      report_metric('Memory/Main/Resident', 'bytes', stats['mem']['resident'] * 1024 * 1024)
      report_metric('Memory/Main/Virtual', 'bytes', stats['mem']['virtual'] * 1024 * 1024)
      report_metric('Memory/Main/Mapped', 'bytes', stats['mem']['mapped'] * 1024 * 1024)
      report_metric('Memory/Main/Mapped with Journal', 'bytes', stats['mem']['mappedWithJournal'] * 1024 * 1024)

      ## tcmalloc
      report_section('Memory/Tcmalloc', stats['tcmalloc']['generic'])
      report_section('Memory/Tcmalloc', stats['tcmalloc']['tcmalloc'])

      # Locks
      ## Global
      report_lock_metric('Locks/Global/Count/Acquiring Shared (S) Lock', 'count', stats, 'locks|Global|acquireCount|R')
      report_lock_metric('Locks/Global/Count/Acquiring Intent Shared (IS) Lock', 'count', stats, 'locks|Global|acquireCount|r')
      report_lock_metric('Locks/Global/Count/Acquiring Exclusive (X) Lock', 'count', stats, 'locks|Global|acquireCount|W')
      report_lock_metric('Locks/Global/Count/Acquiring Intent Exclusive (IX) Lock', 'count', stats, 'locks|Global|acquireCount|w')

      report_lock_metric('Locks/Global/Wait Count/Acquiring Shared (S) Lock', 'count', stats, 'locks|Global|acquireWaitCount|R')
      report_lock_metric('Locks/Global/Wait Count/Acquiring Intent Shared (IS) Lock', 'count', stats, 'locks|Global|acquireWaitCount|r')
      report_lock_metric('Locks/Global/Wait Count/Acquiring Exclusive (X) Lock', 'count', stats, 'locks|Global|acquireWaitCount|W')
      report_lock_metric('Locks/Global/Wait Count/Acquiring Intent Exclusive (IX) Lock', 'count', stats, 'locks|Global|acquireWaitCount|w')

      report_lock_metric('Locks/Global/Time/Acquiring Shared (S) Lock', 'microseconds', stats, 'locks|Global|timeAcquiringMicros|R')
      report_lock_metric('Locks/Global/Time/Acquiring Intent Shared (IS) Lock', 'microseconds', stats, 'locks|Global|timeAcquiringMicros|r')
      report_lock_metric('Locks/Global/Time/Acquiring Exclusive (X) Lock', 'microseconds', stats, 'locks|Global|timeAcquiringMicros|W')
      report_lock_metric('Locks/Global/Time/Acquiring Intent Exclusive (IX) Lock', 'microseconds', stats, 'locks|Global|timeAcquiringMicros|w')

      ## Database
      report_lock_metric('Locks/Database/Count/Acquiring Shared (S) Lock', 'count', stats, 'locks|Database|acquireCount|R')
      report_lock_metric('Locks/Database/Count/Acquiring Intent Shared (IS) Lock', 'count', stats, 'locks|Database|acquireCount|r')
      report_lock_metric('Locks/Database/Count/Acquiring Exclusive (X) Lock', 'count', stats, 'locks|Database|acquireCount|W')
      report_lock_metric('Locks/Database/Count/Acquiring Intent Exclusive (IX) Lock', 'count', stats, 'locks|Database|acquireCount|w')

      report_lock_metric('Locks/Database/Wait Count/Acquiring Shared (S) Lock', 'count', stats, 'locks|Database|acquireWaitCount|R')
      report_lock_metric('Locks/Database/Wait Count/Acquiring Intent Shared (IS) Lock', 'count', stats, 'locks|Database|acquireWaitCount|r')
      report_lock_metric('Locks/Database/Wait Count/Acquiring Exclusive (X) Lock', 'count', stats, 'locks|Database|acquireWaitCount|W')
      report_lock_metric('Locks/Database/Wait Count/Acquiring Intent Exclusive (IX) Lock', 'count', stats, 'locks|Database|acquireWaitCount|w')

      report_lock_metric('Locks/Database/Time/Acquiring Shared (S) Lock', 'microseconds', stats, 'locks|Database|timeAcquiringMicros|R')
      report_lock_metric('Locks/Database/Time/Acquiring Intent Shared (IS) Lock', 'microseconds', stats, 'locks|Database|timeAcquiringMicros|r')
      report_lock_metric('Locks/Database/Time/Acquiring Exclusive (X) Lock', 'microseconds', stats, 'locks|Database|timeAcquiringMicros|W')
      report_lock_metric('Locks/Database/Time/Acquiring Intent Exclusive (IX) Lock', 'microseconds', stats, 'locks|Database|timeAcquiringMicros|w')

      ## Collection
      report_lock_metric('Locks/Collection/Count/Acquiring Shared (S) Lock', 'count', stats, 'locks|Collection|acquireCount|R')
      report_lock_metric('Locks/Collection/Count/Acquiring Intent Shared (IS) Lock', 'count', stats, 'locks|Collection|acquireCount|r')
      report_lock_metric('Locks/Collection/Count/Acquiring Exclusive (X) Lock', 'count', stats, 'locks|Collection|acquireCount|W')
      report_lock_metric('Locks/Collection/Count/Acquiring Intent Exclusive (IX) Lock', 'count', stats, 'locks|Collection|acquireCount|w')

      ## Metadata
      report_lock_metric('Locks/Metadata/Count/Acquiring Shared (S) Lock', 'count', stats, 'locks|Metadata|acquireCount|R')
      report_lock_metric('Locks/Metadata/Count/Acquiring Intent Shared (IS) Lock', 'count', stats, 'locks|Metadata|acquireCount|r')
      report_lock_metric('Locks/Metadata/Count/Acquiring Exclusive (X) Lock', 'count', stats, 'locks|Metadata|acquireCount|W')
      report_lock_metric('Locks/Metadata/Count/Acquiring Intent Exclusive (IX) Lock', 'count', stats, 'locks|Metadata|acquireCount|w')

      ## Oplog
      report_lock_metric('Locks/Oplog/Count/Acquiring Shared (S) Lock', 'count', stats, 'locks|oplog|acquireCount|R')
      report_lock_metric('Locks/Oplog/Count/Acquiring Intent Shared (IS) Lock', 'count', stats, 'locks|oplog|acquireCount|r')
      report_lock_metric('Locks/Oplog/Count/Acquiring Exclusive (X) Lock', 'count', stats, 'locks|oplog|acquireCount|W')
      report_lock_metric('Locks/Oplog/Count/Acquiring Intent Exclusive (IX) Lock', 'count', stats, 'locks|oplog|acquireCount|w')

      # Global Lock
      report_lock_metric('Global Lock/Lock Total Time', 'microseconds', stats, 'globalLock|totalTime')

      report_section('Global Lock/Current Queue', stats['globalLock']['currentQueue'])
      report_section('Global Lock/Active Clients', stats['globalLock']['activeClients'])

      @prior = stats

      # DBStats metrics
      report_db_stats()

      # WiredTiger engine metrics
      if stats.key?('wiredTiger')
        report_wiredtiger_stats(stats['wiredTiger'])
      end

    rescue => e
      $stderr.puts "#{e}: #{e.backtrace.join("\n   ")}"
    end

    def client
      @client ||= begin
        client = MongoClient.new(endpoint, port.to_i, :slave_ok => true, :ssl => ssl || false)

        unless username.nil?
          client.db('admin').authenticate(username, password)
        end
        client
      end
    rescue Mongo::AuthenticationError
      $stderr.puts 'Error authententicating to MongoDB database. Requires a user on the admin database'
      exit 1
    rescue Mongo::ConnectionFailure
      $stderr.puts "Error connecting to host port provided: #{endpoint}:#{port}"
      exit 1
    end

    def mongodb_server_stats
      client.db('local').command('serverStatus' => 1)
    end

    def report_db_stats
      for db_name in @client.database_names
        db_stats = @client.db(db_name).stats

        report_metric("DBStats/Collections/#{db_name}", 'Collections', db_stats['collections'])
        report_metric("DBStats/Objects/#{db_name}", 'Objects', db_stats['objects'])
        report_metric("DBStats/Indexes/#{db_name}", 'Indexes', db_stats['indexes'])

        report_metric("DBStats/Average Object Size/#{db_name}", 'Size', db_stats['avgObjSize'])
        report_metric("DBStats/Data Size/#{db_name}", 'bytes', db_stats['dataSize'])
        report_metric("DBStats/Storage Size/#{db_name}", 'bytes', db_stats['storageSize'])
        report_metric("DBStats/Index Size/#{db_name}", 'bytes', db_stats['indexSize'])
      end
    end

    def report_wiredtiger_stats(wt_stats)
      %w(LSM async block-manager cache connection cursor data-handle log reconciliation session thread-yield transaction).each do |key|
        section_name = key.split('-').collect(&:capitalize).join(' ').squeeze(' ').strip
        report_section("Wired Tiger/#{section_name}", wt_stats[key])
      end

      report_section("Wired Tiger/Concurrent Transactions/Read", wt_stats['concurrentTransactions']['read'])
      report_section("Wired Tiger/Concurrent Transactions/Write", wt_stats['concurrentTransactions']['write'])
    end

    def report_section(metric_name_prefix, section_stats, units = 'count')
      section_stats.each do |key, value|
        key_name = key.sub('-', '_').split('_').collect(&:capitalize).join(' ').squeeze(' ').strip
        if key.include?('bytes')
          units = 'bytes'
        end
        report_metric(metric_name_prefix + "/#{key_name}", units, value)
      end
    end

    def report_counter_metric(metric_name, units, value)
      @counter_metrics ||= {}

      if @counter_metrics[metric_name].nil?
        @counter_metrics[metric_name] = NewRelic::Processor::EpochCounter.new
      end

      report_metric(metric_name, units, @counter_metrics[metric_name].process(value))
    end

    def report_lock_metric(metric_name, units, stats, path)
      current_value = path.split(/\|/).inject(stats) { |v, p| v[p] }
      if current_value.nil?
        current_value = 0
      end

      if @prior
        prior_value = path.split(/\|/).inject(@prior) { |v, p| v[p] }
        if prior_value.nil?
          prior_value = 0
        end

        uptime_millis = (stats['uptimeMillis'] - @prior['uptimeMillis']) * 1.0
        lock_ratio = (current_value - prior_value) / uptime_millis / (uptime_millis / 1000)

        report_metric(metric_name, units, lock_ratio)
      end
    end

  end

  #
  # Register this agent.
  #
  NewRelic::Plugin::Setup.install_agent :mongodb, self

  #
  # Launch the agent; this never returns.
  #
  NewRelic::Plugin::Run.setup_and_run

end
