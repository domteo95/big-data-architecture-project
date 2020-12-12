'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const url = require('url');
const port = Number(process.argv[2]);

const hbase = require('hbase')
var hclient = hbase({ host: process.argv[3], port: Number(process.argv[4])})

function counterToNumber(c) {
	return Number(Buffer.from(c).readBigInt64BE());
}

function rowToMap(row) {
	var stats = {}
	row.forEach(function (item) {
		if (item['column'] == "stats:fifa_version" || item['column'] == "stats:year") {
			stats[item['column']] = item['$']
			// because the fifa_version column value is a string and not a number
		} else {
			stats[item['column']] = counterToNumber(item['$'])
		}
	});
	return stats;
}

hclient.table('dominicteo_hbase_proj_team_v2').row('Manchester United!2010').get((error, value) => {
	console.info(rowToMap(value))
	//console.info(value)
})


app.use(express.static('public'));

function removePrefix(text, prefix) {
	if(text.indexOf(prefix) != 0) {
		throw "missing prefix"
	}
	return text.substr(prefix.length)
}
app.get('/fifa-stats-teams.html', function (req, res) {
	hclient.table('dominicteo_hbase_proj_unique').scan({ maxVersions: 1}, (err,rows) => {
		var template = filesystem.readFileSync("fifa-teams-unique.mustache").toString();
		var html = mustache.render(template, {
			teams : rows
		});
		res.send(html)
	})
});
app.get('/fifa-stats-results.html',function (req, res) {
	const team=req.query['team']+ "!";
	console.log("TEAM", team);
	function processYearRecord(yearRecord) {
		console.log("YEARRECORD", yearRecord)
		var result = { year : yearRecord['year']};

		["num_players", "overall_ratings", "potential_ratings", "passing_ratings", "pace_ratings", "shooting_ratings",
			"defending_ratings", "physical_ratings", "dribbling_ratings", "european_players", "african_players",
			"samerican_players", "namerican_players", "oceania_players", "asian_players", "values_money",
			"wages_money", "country_ratings", "fifa_version"].forEach(stat => {
			var num_players = Number(yearRecord['num_players'])
			result["club"] = team.slice(0,-1)
			if (stat == "fifa_version") {
				result[stat] = yearRecord[stat]
			} else if (stat.includes('num')){
				result[stat] = num_players
			} else if (stat.includes('ratings')){
				var stats = yearRecord[stat]
				result[stat] = Number((stats/num_players).toFixed(0))
			} else if (stat.includes('players')){
				result[stat] = yearRecord["num_"+stat]
			} else if (stat.includes('money')){
				if (yearRecord[stat]==0){
					result[stat] = ' - '
				} else {
					var stats = yearRecord[stat]
					result[stat] = Number((stats/num_players).toFixed(0))
				}
			}
		})
		return result;
	}
	function teamInfo(cells) {
		var result = [];
		var yearRecord;
		cells.forEach(function(cell) {
			var year = Number(removePrefix(cell['key'], team))
			if(yearRecord === undefined)  {
				yearRecord = { year: year }
			} else if (yearRecord['year'] != year ) {
				result.push(processYearRecord(yearRecord))
				yearRecord = { year: year }
			}
			if (cell['column'] == 'stats:fifa_version'){
				yearRecord[removePrefix(cell['column'],'stats:')] = cell['$']
			} else if (cell['column'] == 'stats:year') {
				yearRecord[removePrefix(cell['column'],'stats:')] = Number(cell['$'])
			} else {
					yearRecord[removePrefix(cell['column'],'stats:')] = counterToNumber(cell['$'])
				}

		})
		result.push(processYearRecord(yearRecord))

		return result;
	}
	hclient.table('dominicteo_hbase_proj_team_v2').scan({
			filter: {type : "PrefixFilter",
				value: team },
			maxVersions: 1},
		(err, cells) => {
			console.info('CELLS', cells);
			var ti = teamInfo(cells);
			console.info('TEAMINFO', ti);
			var template = filesystem.readFileSync("fifa-stats-results.mustache").toString();
			var html = mustache.render(template, {
				teamInfo : ti,
				team : team
			});
			res.send(html)

		})
});



app.get('/submit-player.html', function (req, res) {
	hclient.table('dominicteo_hbase_proj_unique').scan({ maxVersions: 1}, (err,rows) => {
		var template = filesystem.readFileSync("fifa-submit-player.mustache").toString();
		var html = mustache.render(template, {
			teams : rows
		});
		res.send(html)
	})
});

var kafka = require('kafka-node');
var Producer = kafka.Producer;
var KeyedMessage = kafka.KeyedMessage;
var kafkaClient = new kafka.KafkaClient({kafkaHost: process.argv[5]});
var kafkaProducer = new Producer(kafkaClient);

app.get('/new-player.html',function (req, res) {
	var team_val = req.query['team'];
	var european_val = (req.query['european']) ? true : false;
	var african_val = (req.query['african']) ? true : false;
	var namerican_val = (req.query['namerican']) ? true : false;
	var samerican_val = (req.query['samerican']) ? true : false;
	var asian_val = (req.query['asian']) ? true : false;
	var oceania_val = (req.query['oceania']) ? true : false;
	var potential_val =  req.query['potential'];
	var overall_val = req.query['overall'];
	var pace_val = req.query['pace'];
	var defending_val = req.query['defending'];
	var shooting_val = req.query['shooting'];
	var passing_val = req.query['passing'];
	var dribbling_val = req.query['dribbling'];
	var physical_val = req.query['physical'];
	var wages_val = req.query['wages']
	var country_val = req.query['country']
	var year_val = req.query['year']
	var report = {
		team_year : team_val +  '!' + year_val,
		european : european_val,
		african: african_val,
		namerican: namerican_val,
		samerican: samerican_val,
		asian: asian_val,
		oceania: oceania_val,
		potential: potential_val,
		overall: overall_val,
		pace: pace_val,
		defending: defending_val,
		shooting: shooting_val,
		passing: passing_val,
		dribbling: dribbling_val,
		physical: physical_val,
		wages: wages_val,
		country: country_val
	};

	console.log("Player Report", report)
	kafkaProducer.send([{ topic: 'dominicteo-new-players-v3', messages: JSON.stringify(report)}],
		function (err, data) {
			console.log("Kafka Error: " + err)
			console.log(data);
			console.log(report);
		});
});

app.listen(port);
