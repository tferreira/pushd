Subscriber = require('./subscriber').Subscriber
Event = require('./event').Event

class Statistics
    constructor: (@redis) ->

    collectStatistics: (cb) ->
        @getPublishedCounts (totalPublished, publishedOSDaily, totalErrors, errorsOSDaily) =>
            Subscriber::subscriberCount @redis, (numsubscribers, subscribersPerProto) =>
                Event::eventCount @redis, (numevents) =>
                    stats =
                        totalSubscribers: numsubscribers
                        subscribers: subscribersPerProto
                        totalPublished: totalPublished
                        published: publishedOSDaily
                        totalErrors: totalErrors
                        errors: errorsOSDaily
                        totalEvents: numevents
                    cb(stats) if cb

    increasePublishedCount: (proto, countIncreament, cb) ->
        keyname = @publishedKeyname(proto)
        @redis.incrby(keyname, countIncreament)
        cb() if cb

    increasePushErrorCount: (proto, countIncreament, cb) ->
        keyname = @errorKeyname(proto)
        @redis.incrby(keyname, countIncreament)
        cb() if cb

    getPublishedCounts: (cb) ->
        @redis.keys @allPublishedKeyname(), (err, publishedKeys) =>
            @redis.keys @allErrorsKeyname(), (err, errorKeys) =>
                @getOSDailyCounts publishedKeys, (totalPublished, publishedCounts) =>
                    @getOSDailyCounts errorKeys, (totalErrors, errorCounts) =>
                        cb(totalPublished, publishedCounts, totalErrors, errorCounts) if cb

    getOSDailyCounts: (keys, cb) ->
        if keys.length == 0
            cb(0, {}) if cb
            return

        @redis.mget keys, (err, values) =>
            countsOSDaily = {}
            total = 0
            keys.forEach (key, i) =>
                dateAndProto = key.split(':').slice(-2)
                day = dateAndProto[0]
                todayKeyName = @publishedKeynamePostfix(proto).split(':').slice(-2)
                todayDate = todayKeyName[0]
                proto = dateAndProto[1]
                if not countsOSDaily[proto]?
                    countsOSDaily[proto] = {}
                x = parseInt values[i], 10
                if day == todayDate
                    countsOSDaily[proto]['today'] = x
                else
                    countsOSDaily[proto]['today'] = 0
                total += x

            cb(total, countsOSDaily) if cb

    clearPublishedCounts: (cb) ->
        @redis.keys @allPublishedKeyname(), (err, statsKeys) =>
            if statsKeys?
                @redis.del statsKeys

            @redis.keys @allErrorsKeyname(), (err, errorKeys) =>
                if errorKeys?
                    @redis.del errorKeys

                cb() if cb

    publishedKeyname: (proto) ->
        return 'statistics:published:' + @publishedKeynamePostfix(proto)

    allPublishedKeyname: ->
        return 'statistics:published:*'

    errorKeyname: (proto) ->
        return 'statistics:pusherrors:' + @publishedKeynamePostfix(proto)

    allErrorsKeyname: ->
        return 'statistics:pusherrors:*'

    publishedKeynamePostfix: (proto) ->
        today = new Date
        year = today.getUTCFullYear().toString()
        month = (today.getUTCMonth() + 1).toString()
        day = (today.getUTCDate() + 1).toString()
        if month.length < 2
            month = '0' + month
        if day.length < 2
            day = '0' + day
        return "#{year}-#{month}-#{day}:#{proto}"

exports.Statistics = Statistics
