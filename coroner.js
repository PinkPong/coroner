/*require('undead/undead').undead(function() {
	doLog('panic!!! somebody want to kill us...');
	process.stdin.resume();
	self.stop();
	dumpster.stop();
	console.log('coroner: i`m done');
});*/

/*==========

    new CronJob('* * * * * *', function(){
        console.log('You will see this message every second');
    }, null, true, "America/Los_Angeles");


Available Cron patterns:
==========*/

    //Asterisk. E.g. *
    //Ranges. E.g. 1-3,5
    //Steps. E.g. */2   

/*[Read up on cron patterns here](http://crontab.org).

Cron Ranges
==========

When specifying your cron values you'll need to make sure that your values fall within the ranges. For instance, some cron's use a 0-7 range for the day of week where both 0 and 7 represent Sunday. We do not.

 * Seconds: 0-59
 * Minutes: 0-59
 * Hours: 0-23
 * Day of Month: 1-31
 * Months: 0-11
 * Day of Week: 0-6
==========*/

/* 
*	vars:
*
*	cronDate:Date	- current date
*	cronStart:Date	- cron start date
*	cronShift:int	- diff from cronStart to cronDate
*	cronCount:int	- number of cron runs 1,...,xxx
*
*	operations:
*	+ - ,
*
*
***/


/* 
*	config section
***/
var addJobRq = 'fresh-meat-';
var delJobRq = 'clean-up-';
var fJobs = 'jobs';
var fLog = 'master-log';
var fJobsLog = 'waste';
var fLocation = 'dumpster';
var tz = "America/Los_Angeles";
var selfSchedule = '* * * * * *';	// '*/30 * * * * *';
var backupSchedule = '06 06 00 * * 0-6';
var dailyTime = '04 04 04 * * *';	// '00 10 0,4,8,12,16,20 * * *'
var weeklyTime = '05 05 05 * *';	// day needs to be added at init time!
var port = 3030;

var http = require('http');
var express = require('express');
var bodyParser = require('body-parser');
var url = require('url');

var app = express();
app.use(bodyParser.urlencoded({extended: true}));
app.set('port', process.env.PORT || port);

var fs = require('fs');
var exec = require('child_process').exec;
var CronJob = require('cron').CronJob;

var time = 0;
var masterLog;
var waste;
var activeDumpster;
var jobList = {};
var activeJobs = {
		daily: {
			active: true,
			jobs: {},
			schedule: dailyTime
		},
		weekly: {
			active: true,
			jobs: {},
			schedule: weeklyTime
		},
		custom: {
			active: true,
			jobs: {},
			schedule: ''
		}
	};

var pad = function (num, pads) {
	var rez = '';
	if (num<10) { rez = '0'; }
	if ((num<100) && (pads>1)) { rez += '0'; }
	if ((num<1000) && (pads>2)) { rez += '0'; }
	return rez + num + '';
}

var _timer = function() {
	var t = new Date();
	var diff = (t.valueOf() - time)/1000;
	time = t.valueOf();
	var lt = t.getFullYear() + '-' + pad(t.getMonth() + 1) + '-' + pad(t.getDate()) + ' ' + pad(t.getHours()) + ':' + pad(t.getMinutes()) + ':' + pad(t.getSeconds());
	var dt = t.toISOString().split('.')[0].split('T')
	return [dt[0]+' '+dt[1]+' GMT', diff, lt];
}	

var self = new CronJob(selfSchedule, function() {
	/*
	* Runs based on selfSchedule time
	*/
	//console.log(_timer()[2], 'got meat?');
	getFreshMeat();
}, function(){doLog('coroner. INF >> i`m terminated')}, false, tz);


var _cmd = function(cmd, job) { 

	console.log('--- cmd ---');
	console.log('CID:' + job.id, cmd);
	
	doLog('CID:' + job.id + '. INF >> CMD -->' + cmd, waste);
	exec(cmd + ' ' + job.id, function(err, stdout, stderr) {
		if(err) {
			doLog('CID:' + job.id + '. ERR >> ' + err, waste);
			console.log('err', err);
		} else {
			doLog('CID:' + job.id + '. REZ >>' + stdout, waste);
			console.log('stdout', stdout);
		}
	});
};

var _post = function(data, job) {
	doLog('CID:' + job.id + '. INF >> POST --> ...This type of crons are not supported yet...');
}

var _get = function(uri, job) {
	
	var _http;
	
	if (uri.indexOf('https://') !== -1) {
		_http = require('https');
		process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = '0';
	} else {
		_http = http;
	}
	
	if (_http !== undefined) {
		doLog('CID:' + job.id + '. INF >> GET -->' + uri, waste);
		_http.get(uri + '&coronerid=' + job.id, function(res) {
			var rez = '';
			res.on('data', function(d) {
				rez += d;
			});

			res.on('end', function() {
				/*try {
					var rez = JSON.parse(body);
				} catch (err) {
					console.error('Unable to parse response as JSON', err);
					doLog(err,waste);
					return;
				}*/
				doLog('CID:' + job.id + '. REZ >> ' + rez.toString(), waste);
			});
		}).on('error', function(err) {
			// handle errors with the request itself
			console.error('err', err.message);
			doLog('CID:' + job.id + '. ERR >> ' + err.toString(), waste);
		});
	}
}


var dumpster = new CronJob(backupSchedule, function() {
	/*
	* Runs accoring to backupSchedule
	*/
	self.stop();
	console.log('dumpster. INF >> start recycling');
	fs.rename(fLog, activeDumpster + '/' + fLog, function (err) {
		if (err) {
			doLog('dumpster. ERR >> master-log backup failed');
			doLog(err);
			} else {
			fs.rename(fJobsLog, activeDumpster + '/' + fJobsLog, function (err) {
				if (err) {
					doLog('dumpster. INF >> waste backup failed');
					doLog(err);
					} else {
					fs.readFile(fJobs, 'utf8', function(err, data) {
						if (err) {
							doLog('dumpster. INF >> jobs backup failed (no reaad access)');
							} else {
							fs.writeFile(activeDumpster + '/' + fJobs, data, function( err) {
								if (err) {
									doLog('dumpster. INF >> jobs backup failed (can not save jobs file)');
									} else {
										checkDumpster();
										init(true);
										console.log('dumpster: finished');
										doLog('dumpster. REZ >> recycling finished');
										self.start();
								}
							});
						}
					});
				}
			});
		}
	});
	
  }, function(){doLog('dumpster. INF >> i`m terminated')}, false, tz);


var addJob = function (jobData, f) {
	doLog('coroner. INF >> incoming job: ' + jobData);

	var jobID = (new Date()).valueOf();
	var tStamp = Number(jobData.substr(0,13));
	var save = true;
	var reject = false;
	var rReason = '';
	if (!isNaN(tStamp)) {
		jobID = tStamp;
		jobData = jobData.substr(13);
		save = false;
	}
	
	//var _job = JSON.parse(jobData);
	var _job = {};
	try {
		_job = JSON.parse(jobData);
	} catch (err) {
		rReason += 'unable to parse job file;';
		console.error('unable to parse job request', err);
	}
	console.log('validating & procesing new job ->', jobID);//, _job);
	
	// validate job
	if (_job.schedule !== undefined) {
		if ((_job.schedule !== 'daily') && (_job.schedule !== 'weekly') && (_job.schedule.split(' ').length !== 6)) {
			rReason += 'wrong job schedule format;';
		}
	} else {
		rReason += 'schedule type is unknown;';
	}
	
	if ((_job.cronStart === undefined) || isNaN(Date.parse(_job.cronStart+':00:00:00'))) {
		rReason += 'cronStart is unknown or invalid;';
	}
	
	if ((_job.get === undefined) && (_job.post === undefined) && (_job.cmd === undefined)) {
		rReason += 'cron type is unknown;';
	}	

	if (rReason !== '') {
		save = false;
		reject = true;
	}
	if (save) {
		fs.rename(f, activeDumpster+'/'+f, function (err) {
			if(err) {
				doLog(err);
			}
		});
	}
	
	if (reject) {
		console.log('CID:' + jobID + ' rejected (' + rReason + ')');
		doLog('CID:' + jobID + ' REZ >> rejected (' + rReason + ')');
		fs.rename(f, 'rejected-' + f, function (err) {
			if(err) {
				doLog(err);
			}
		});
	} else {
		if (save) {
			var j = '' + jobID + jobData + '\n';
			fs.write(jobs, j, 0, 0, function (err, written, string) {
				jobList[jobID] = _job;
				initJob(jobID);
				//console.log('jobList--->',jobList,Object.keys(jobList),Object.keys(jobList).length);
			});
		} else {
			if (jobList[jobID] === undefined) {
				jobList[jobID] = _job;
				initJob(jobID);
			}
			//console.log('jobList--->',jobList,Object.keys(jobList),Object.keys(jobList).length);
		}
	}
}

var runJob = function(data) {
	console.log('run job->',data);
}

var spawnCron = function(data) {
	var _cron = {};
	
	_cron.data = data;
	
	var cronSchedule = '';
	
	switch (data.schedule) {
		case 'daily': 
			cronSchedule = activeJobs[data.schedule].schedule;
		break;
		case 'weekly':
			var wd = new Date(Date.parse(data.cronStart + ':00:00:00'));
			cronSchedule = activeJobs[data.schedule].schedule + wd.getDay();
		break;
		default:
			cronSchedule = data.schedule;
		break;
	}
	
	//_cron.cron = new CronJob('*/10 * * * * *', function() {
	_cron.cron = new CronJob(cronSchedule, function() {
		
		var d = new Date();
		var cronDate = d.getFullYear() + '-' + pad(Number((d.getMonth() + 1))) + '-' + pad(Number(d.getDate()));	// current date
		var cronStart = _cron.data.cronStart;	// cron start date
		//var cronShift = Math.round(((Date.parse(cronDate+':00:00:00')).valueOf() -  (Date.parse(cronStart+':00:00:00')).valueOf())/(864000007));	// diff from cronStart to cronDate
		var cronShift = Math.round(((Date.parse(cronDate)).valueOf() -  (Date.parse(cronStart)).valueOf())/(86400000));	// diff from cronStart to cronDate
		
		_cron.cronDate = cronDate;
		_cron.cronStart = cronStart;
		_cron.cronShift = cronShift;
		_cron.cronSchedule = cronSchedule;

		//console.log('cron data----->',_cron.data);
		var type = '';
		var types = ['get','post','cmd'];
		for (var t=0; t<types.length; t++) {
			if (_cron.data[types[t]] !== undefined) {
				type = types[t];
			}
		}
	
		var _template = _cron.data[type];
		_template=_template.replace(/{[a-z0-9\+\-,]+\}/gi, function(_q) { 
					//console.log('_q----->',_q);//,_p,_d);
					var tmp = _q.substr(1,_q.length-2);
					var vars = tmp.split(',');
					var vLn = vars.length;
					for (var v=0; v<vLn; v++) {
						var _tmp = vars[v].trim();
						
						var ops = {
							ops: ['+'],//,'-'],
							op: '!',
							_ops: [],
							type: 'string'
						}
						
						_tmp = _tmp.split('-').join('+-');
						
						for (var o=0; o<ops.ops.length; o++) {
							if (_tmp.indexOf(ops.ops[o]) !== -1) {
								ops.op = ops.ops[o];
							}
						}
						
						var __q = _tmp.split(ops.op);
						var maxDate = 31556995200000;	// year 2970
						
						for (var q=0; q<__q.length; q++) {
							var _val = _cron.data[__q[q]] || _cron[__q[q]];
							
							if(_val === undefined) {
								_val = __q[q];
							}
							
							var dVal = Date.parse(_val + ':00:00:00');
							if ((dVal>0) && (dVal<maxDate) && ((_val instanceof String) || (typeof(_val) === 'string'))) {
								ops.type = 'date';
							} else {
								if (!isNaN(_val) && (ops.type !== 'date') ) {
									ops.type = 'number';
								}
							}
								
							ops._ops.push(_val);
						}
						//console.log('-----',ops,ops.type);
						
						if (ops.type === 'date') {
							for (var o=0; o<ops._ops.length; o++) {
								var dateVal = Date.parse(ops._ops[o] + ':00:00:00');
								//console.log(dateVal);
								if (isNaN(dateVal) || (dateVal<0)) {	//cronShift
									dateVal = ops._ops[o]*86400000;//24*3600000
								}
								ops._ops[o] = dateVal;
							}
						}
						
						//console.log('+++++++',ops);
						
						if ((ops.type === 'string')&&(ops.op === '+')) {
							_tmp = ops._ops.join('');
						} else {
							var rez = ops._ops[0];
							for (var o=1; o<ops._ops.length; o++) {
								/*switch (ops.op) {
									case '+':
										rez += Number(ops._ops[o]);
									break;
									case '-':
										rez += Number(ops._ops[o]);
									break;
								}*/
								rez += Number(ops._ops[o]);
							}
							_tmp = rez;
						}
						
						if (ops.type === 'date') {
							//console.log(_tmp);
							var t = new Date(_tmp);
							//console.log(t);
							_tmp = t.toISOString().split('T')[0];
						}
						vars[v] = _tmp;
					}
					_q = vars.join(',');
					return _q;
				});

		console.log('........................');
		console.log('cron id ............. >>',_cron.data.id);
		console.log('cron schedule ....... >>',_cron.data.schedule);
		console.log('cron cronSchedule ... >>',cronSchedule);
		console.log('cron memo ........... >>',_cron.data.memo);
		console.log('cron type ........... >>',type);
		console.log('cron self ........... >>',_cron.data[type]);
		console.log('cron _template ...... >>',_template);
		console.log('cron cronStart ...... >>',cronStart);//,(new Date(Date.parse(cronStart))).toISOString());
		console.log('cron cronDate ....... >>',cronDate);//,(new Date(Date.parse(cronDate))).toISOString());
		console.log('cron cronShift ...... >>',cronShift);
		console.log('........................');
		
		switch (type) {
			case 'cmd':
				_cmd(_template, _cron.data);
			break;
			case 'get':
				_get(_template, _cron.data);
			break;
			case 'post':
				_post(_cron.data, _cron.data);
			break;
		}
	}, function(){doLog('CID:' + _cron.data.id + '. INF >> i`m terminated')}, false, tz);
	
	return _cron;
}

var initJob = function(id) {
	console.log('attempt to init job ID:',id);
	var job = jobList[id];
	if (job !== undefined) {
		console.log('init CID: ' + id + '. ok');
		job.id = id;
		var cron =	spawnCron(job);
		var sch = job.schedule.toLowerCase();
		if ((sch !== 'daily') && (sch !== 'weekly')) {
			sch = 'custom';
		}
		activeJobs[sch].jobs[id] = cron;
		cron.cron.start();
		doLog('CID:' + id + '. INF >> cron spawn - ok');
		/*
		switch (sch) {
			case 'daily':
			case 'weekly':
			case 'custom':
				activeJobs[sch].jobs[id] = cron;
				cron.cron.start();
				doLog('job ID:' + id + ' cron spawn - ok');
			break;
			default:
				console.log('wrong scheduler format');
				doLog('job ID:' + id + ' wrong scheduler format. skipping')
			break;
		}*/
	} else {
		console.log('init failed');
		doLog('CID:' + id + '. ERR >> can not be initialazed');
	}
}

var removeJob = function (jobID, f) {
	doLog('cleaning up job ID:' + jobID);
	if (jobList[jobID] === undefined) {
		doLog('CID:' + jobID + '. ERR >> dosen`t exist');
	} else {
		var job = jobList[jobID];
		var sch = job.schedule.toLowerCase();
		if ((sch !== 'daily') && (sch !== 'weekly')) {
			sch = 'custom';
		}
		activeJobs[sch].jobs[jobID].cron.stop();
		job = null;
		delete jobList[jobID];
		flushJobs();
	}
	/*
	*	delete clean-up
	*/
	fs.rename(f, activeDumpster+'/'+f, function (err) {
		if (err) {
			doLog(err);
		}
	});
}


/*
*	flush jobList to jobs file
*/
var flushJobs = function() {
	fs.open(fJobs, 'w',
		function(err, f) {
			if (err) {
				doLog('coroner. ERR >> jobs file can not be flushed!');
			} else {
				doLog('coroner. REZ >> jobs data flushed');
				jobs = f;
				var j = '';
				for (var jj in jobList) {
					j += jj + JSON.stringify(jobList[jj]) + '\n';
				}
				fs.write(jobs, j, 0, 0, function (err, written, string) {
						//console.log('*****');
						console.log('jobList flushed. active--->',Object.keys(jobList).length);//jobList,Object.keys(jobList),Object.keys(jobList).length);
					});
			}
		});
}

/*
*	write different stuff to log files
*/
var doLog = function(msg, f) {
	if (f === undefined) {
		f = masterLog;
	}
	//console.log('write file',f,msg);
	//try {
	fs.write(f, _timer()[0]+': '+msg+'\n', 0, 0, 0, function (err, written, string) {
		console.log(err, written, string);
	});
	//} catch (e) {console.log(e)}
};

var checkDumpster = function () {
				
	//activeDumpster = 'dumpster/' + _timer()[0].split(' ')[0];
	activeDumpster = fLocation + '/' + _timer()[0].split(':').join('-');

	fs.readdir(activeDumpster, function(err, files) {
		if (err) {
			doLog('dumpster. INF >> no active dumpster, try to create one.');
			fs.mkdir(activeDumpster, function (err) {
				if (err) {
					doLog('dumpster. INF >> active dumpster can not be created, try to create');
					fs.readdir(fLocation, function(err, files) {
						if (err) {
							doLog('dumpster. INF >> no active dumpster place, try to init');
							fs.mkdir(fLocation, function (err) {
								if (err) {
									doLog('dumpster. ERR >> active dumpster place can not be created!');
								} else {
									checkDumpster();
								}
							});
						}
					});				
				}
			});
		}
	});
}

var init = function (backup) {
	_timer();
	
	fs.open(fLog, 'a',
		function(err, f) {
			//console.log(err,f)
			if (!err) {
				masterLog = f;
				doLog('coroner. INF >> --- coroner is watching for you ---');
				fs.open(fJobs, 'a+',
					function(err, f) {
						if (err) {
							doLog('coroner. ERR >> jobs file can not be created!');
						} else {
							jobs = f;
							fs.readFile(fJobs, 'utf8', function(err, data) {
								if (err) {
									doLog('coroner. ERR >> jobs fie can not be accessed!');
								} else {
									var arr = data.split('\n');
									var ctr = 0;
									for (var a=0; a<arr.length; a++) {
										//console.log(a,arr[a])
										if (arr[a] !== '') {
											addJob(arr[a]);
											ctr ++;
										}
									}
									doLog('coroner. REZ >> jobs data parsed. ' + ctr + ' entries found');
								}
								fs.open(fJobsLog, 'a+',
								function(err, f) {
									if (err) {
										doLog('coroner. ERR >> waste file can not be created!');
									} else {
										waste = f;
										doLog('coroner. INF >> start daily waste process');
										if (backup === undefined) {
											checkDumpster();
											dumpster.start();
											self.start();
										}
									}
								});
							});
							
						}
					});
			}
		});
}
	
var getFreshMeat = function() {
	
	var files = fs.readdir('.', function(err, files) {
		//console.log(files);
		var fLn = files.length;
		for (var a=0; a<fLn; a++) {
			var f = files[a].toLowerCase();
			
			if (f.substr(0,11) === addJobRq) {
				var tStamp = new Date(Number(f.substr(11))*1000);
				doLog('coroner. INF >> new job found. [' + f + '] order placed (' + tStamp.toISOString().split('.')[0].split('T').join(' ') + ' GMT)');
				var file = f;	
				fs.readFile(file, 'utf8', function(err, data) {
					//fs.close(file,function(){});
					//console.log('data>>>'+data+'<<<',data)
					//if (data !== '') {
						addJob(data, file);
					//}
				})
			}
			
			if (f.substr(0,9) === delJobRq) {
				var file = f;
				var tStamp = new Date(Number(f.substr(9,10))*1000);
				var id = Number(f.substr(20));
				doLog('coroner. INF >> clean up found [' + file + '] id (' + id + '). order placed (' + tStamp.toISOString().split('.')[0].split('T').join(' ') + ' GMT)');
				removeJob(id, file);
			}
		}		
	});
}


app.get(['/coroner', '/coroner/echo'], function (req, res) {
	console.log('--- echo');
	res.setHeader('Content-Type', 'application/json');
	res.end(JSON.stringify("Hi there! Coroner is alive! Available requests: list, spawn, trash"));
});


app.get('/coroner/spawn', function (req, res) {
	console.log('--- cron spawn rq');
	var q = {callback: 'dummy'};
	try {
		var url_parts = url.parse(req.url, true);
		var q = url_parts.query;
	} catch (e) {
		//console.log(e);
		console.log('err', e);
		res.setHeader('Content-Type', 'application/json');
		res.end(q.callback + '(' + JSON.stringify('err: ' + e) + ')');
		return;
	}

	var jTask = '';
	for (var i in q) {
		if ((jTask === '') ) {
			var b = new Buffer(i, 'base64');
			jTask = b.toString();
		}
	}
	res.setHeader('Content-Type', 'application/json');
	var t = Math.floor(new Date() / 1000).toString();
	var fName = addJobRq + t;
	fs.open('--' + fName, 'w',
		function(err, f) {
			if (err) {
				res.end(q.callback + "(" + JSON.stringify('can not write job task:' + err) + ")");
			} else {
				/*var j = '';
				for (var jj in jobList) {
					j += jj + JSON.stringify(jobList[jj]) + '\n';
				}*/
				fs.write(f, jTask, 0, 0, function (err, written, string) {
						res.end(q.callback + "(" + JSON.stringify('rez:ok') + ")");
						fs.close(f, function (err) {
							fs.rename('--' + fName,fName);
							});
					});
			}
		});
});

app.get('/coroner/trash', function (req, res) {
	console.log('--- cron trash rq');
	var q = {callback: 'dummy'};
	try {
		var url_parts = url.parse(req.url, true);
		var q = url_parts.query;
	} catch (e) {
		//console.log(e);
		console.log('err', e);
		res.setHeader('Content-Type', 'application/json');
		res.end(q.callback + '(' + JSON.stringify('err: ' + e) + ')');
		return;
	}

	var id = '000';
	for (var i in q) {
		if ((id === '000') && !isNaN(i)) {
			id = i;
		}
	}
	res.setHeader('Content-Type', 'application/json');
	var t = Math.floor(new Date() / 1000).toString();
	fs.open(delJobRq + t + '-' + id, 'a+',
		function(err, f) {
				if (err) {
					res.end(q.callback + "(" + JSON.stringify('can not write clean-up:' + err) + ")");
				} else {
					res.end(q.callback + "(" + JSON.stringify('rez:ok') + ")");
					fs.close(f);
				}
			});
});
 
app.get('/coroner/list', function (req, res) {
	console.log('--- crons list rq');
	var q = {callback: 'dummy'};
	try {
		var url_parts = url.parse(req.url, true);
		//console.log('url_parts',url_parts);
		var q = url_parts.query;
	} catch (e) {
		//console.log(e);
		console.log('err', e);
		res.setHeader('Content-Type', 'application/json');
		res.end(q.callback + '(' + JSON.stringify('err:' + e) + ')');
		return;
	}
	res.setHeader('Content-Type', 'application/json');
	res.end(q.callback + '(' + JSON.stringify(jobList) + ')');

	//res.cb = q.callback;
	//getList(Number(q.uuid), res);
});

app.get('*', function (req, res) {
	res.end();
});

var serv = http.createServer(app);
serv.setTimeout(0);
serv.on('connection', function (socket) {});
serv.listen(port);
init();


/*
var cluster = require('cluster');
var numCPUs = require('os').cpus().length;
var maxCPUs = 3;
if (numCPUs > maxCPUs) {
	numCPUs=maxCPUs;
}

if (cluster.isMaster) {
	for (var i = 0; i < numCPUs; i++) {
		cluster.fork();
	}
	cluster.on('exit', function (worker, code, signal) {
		console.log('worker ' + worker.process.pid + ' died');
	});
} else {
	console.log('coroner watch for you on port ' + port + ' on ' + numCPUs + ' CPUs.');
}
*/
