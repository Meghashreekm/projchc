var express = require('express');
var bodyParser = require('body-parser');
var app = express();
const port = 5000;
var cors = require('cors');
const { Client } = require('pg');
app.use(cors());
app.use(bodyParser.json())
const cron = require("node-cron");
const client = new Client({
    "port": 5432,
    "user": "nikhilgupta",
    "database": "linkchecker",
    "hostname": "localhost"
})
var uuid = require('uuid');
app.post('/source', async function (req, res) {
    // Prepare output in JSON format
    const org = 'trew'

    let response = {
        // form: req.body.form,
        url_type: req.body.form.url_type,
        url: req.body.form.url,
        sourceurl: req.body.form.source,
        xpath: req.body.form.xpath,
        x_path: req.body.form.xpath_type,
        cron: req.body.form.cronExpression,
        broken_anchors: req.body.form.broken_anchors,
        group_links: req.body.form.group_links,
        redirects: req.body.form.redirects,
    }
    var source_id = uuid.v4();
    client.query("BEGIN")
    client.query('INSERT into crawler_source(sourceid,sourcename,active,sourceurl,x_path,in_page_anchors,group_link_type,redirect_capturing,org_id,url_type,xpath_type) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)', [source_id, response.sourceurl, true, response.url_type, response.x_path_type, response.broken_anchors, response.group_links, response.redirects, org, response.url, response.xpath]);
    client.query('INSERT into Scheduled_Jobs(sourceid,sourceurl,cron,org_id) values ($1,$2,$3,$4)', [source_id, response.url, response.cron, org]);
    var date = new Date();
    try {
        var x = "* * * * * *"
        cron.schedule(x, function () {
            var spawn = require("child_process").spawn;
            var childProcess = spawn('python3', [
                'main.py', '114', date
            ]);
        });
        const broken_links_404 = await client.query("select sum(total) from (select count(*) as total from crawler_result inner join url on url.id=crawler_result.id where crawler_result.link_status='404' and source_id=($1) and crawler_result.cron=($2) group by url.link, url.id) crawler_result", [ids, date]);
        //broken anchor
        const broken_anchor = await client.query("select sum(total) from (select count(*) as total from crawler_result inner join url on url.id=crawler_result.id where crawler_result.link_status='broken anchor' and source_id=($1) and crawler_result.cron=($2) group by url.link, url.id) crawler_result", [ids, date]);
        //total link
        const total_links = await client.query("select count(*) from crawler_result where source_id=($1) and cron=($2)", [ids, date]);
        //other issues
        const other_issue = await client.query("select count(*) from crawler_result where source_id=($1) and cron=($2) and link_status in ('ConnectionError','broken anchor','SSLError','ReadTimeout')", [ids, date]);
        // console.log(result);
        var broken_links = broken_anchor + broken_links_404;
        var pages_crawled = await client.query("select count(*) from url where sourceid=($1) and startdate=($2)", [ids, date]);
        var active_links = total_links - broken_links - other_issue;
        client.query('INSERT into job_run(sourceid,sourceurl,lastrun,pages_crawled,total_links,active_links,broken_links,org_id) values ($1,$2,$3,$4,$5,$6,$7,$8)', [ids, response.url, response.date, pages_crawled, total_links, active_links, broken_links, 'trew']);
        client.query("COMMIT");
        res.send("success!!");
        // callme(req,res);
    } catch (ex) {
        console.log(ex);
        console.log('failed ${ex} ')
    }
});




app.get('/dashboard', async (req, res) => {
    const org = 'trew'
    const s = await client.query("select sourceid,lastrun from job_run where org_id=($1) order by lastrun desc limit 1", [org])
    let ids = s.rows[0].sourceid
    let cdate = s.rows[0].lastrun
    // console.log(ids)
    // console.log(cdate)
    const source = await client.query("select sourcename from crawler_source where org_id=($1)", [org])
    const broken_links_404 = await client.query("select sum(total) from (select count(*) as total from crawler_result inner join url on url.id=crawler_result.id where crawler_result.link_status='404' and source_id=($1) and crawler_result.startdate=($2) group by url.link, url.id) crawler_result", [ids, cdate]);
    const broken_anchor = await client.query("select sum(total) from (select count(*) as total from crawler_result inner join url on url.id=crawler_result.id where crawler_result.link_status='broken anchor' and source_id=($1) and crawler_result.startdate=($2) group by url.link, url.id) crawler_result", [ids, cdate]);
    const total_links = await client.query("select count(*) from crawler_result where source_id=($1) and startdate=($2)", [ids, cdate]);
    const other_issue = await client.query("select count(*) from crawler_result where source_id=($1) and startdate=($2) and link_status in ('ConnectionError','broken anchor','SSLError','ReadTimeout')", [ids, cdate]);
    //graph values based on day,month and year
    // console.log(other_issue)
    const date = await client.query("select broken_links,EXTRACT(day from lastrun) as day FROM job_run where sourceid=($1)", [ids]);
    // console.log(date)
    const month = await client.query("select broken_links,EXTRACT(month from lastrun) as month FROM job_run where sourceid=($1)", [ids]);
    const year = await client.query("select broken_links,EXTRACT(year from lastrun) as year FROM job_run where sourceid=($1)", [ids]);
    const broken_link_table = await client.query("select url.link,count(*) from crawler_result inner join url on url.id=crawler_Result.id where crawler_result.source_id=($1)and crawler_result.startdate=($2) and link_status in ('broken anchor','404') group by url.link,url.id ORDER BY count desc limit 10", [ids, cdate])
    const broken_link_table_2 = await client.query("select link, count(*) from crawler_result where link_status in ('404','broken anchor')  and  source_id=($1) and startdate =($2) group by link ORDER BY count desc limit 10 ", [ids, cdate])
    // console.log(broken_link_table.count)
    let obj = {
        source: source.rows,
        broken_links_404: broken_links_404,
        broken_anchor: broken_anchor,
        total_links: total_links,
        other_issue: other_issue,
        date: date.rows,
        month: month.rows,
        year: year.rows,
        broken_link_table: broken_link_table.rows,
        broken_link_table_2: broken_link_table_2.rows,
    }
    // console.log(obj);
    res.send(obj);
});
//lastrun
app.get('/crawlerHistory', async function (req, res) {
    // Prepare output in JSON format
    const history = await client.query("select sourceurl,lastrun,broken_links,pages_crawled from Job_Run where org_id='trew' ")
    // console.log(history);
    res.send(history.rows);
})
app.post('/sourcename', async function (req, res) {
    const org = 'trew'
    let sourcename = req.body.source
    // console.log(sourcename);
    let s = await client.query("select sourceid,lastrun from job_run where org_id=($1) and sourceurl=($2) order by lastrun desc limit 1", [org, sourcename])
    let ids = s.rows[0].sourceid
    let cdate = s.rows[0].lastrun
    // console.log(s)
    let broken_links_404 = await client.query("select sum(total) from (select count(*) as total from crawler_result inner join url on url.id=crawler_result.id where crawler_result.link_status='404' and source_id=($1) and crawler_result.startdate=($2) group by url.link, url.id) crawler_result", [ids, cdate]);
    let broken_anchor = await client.query("select sum(total) from (select count(*) as total from crawler_result inner join url on url.id=crawler_result.id where crawler_result.link_status='broken anchor' and source_id=($1) and crawler_result.startdate=($2) group by url.link, url.id) crawler_result", [ids, cdate]);
    let total_links = await client.query("select count(*) from crawler_result where source_id=($1) and startdate=($2)", [ids, cdate]);
    let other_issue = await client.query("select count(*) from crawler_result where source_id=($1) and startdate=($2) and link_status in ('ConnectionError','broken anchor','SSLError','ReadTimeout')", [ids, cdate]);
    //graph values based on day,month and year
    // console.log(other_issue)
    let date = await client.query("select broken_links,EXTRACT(day from lastrun) as day FROM job_run where sourceid=($1)", [ids]);
    // console.log(date)
    let month = await client.query("select broken_links,EXTRACT(month from lastrun) as month FROM job_run where sourceid=($1) ", [ids]);
    let year = await client.query("select broken_links,EXTRACT(year from lastrun) as year FROM job_run where sourceid=($1) ", [ids]);
    let broken_link_table = await client.query("select url.link,count(*) from crawler_result inner join url on url.id=crawler_Result.id where crawler_result.source_id=($1)and crawler_result.startdate=($2) and link_status in ('broken anchor','404') group by url.link,url.id ORDER BY count desc limit 10", [ids, cdate])
    let broken_link_table_2 = await client.query("select link, count(*) from crawler_result where link_status in ('404','broken anchor')  and  source_id=($1) and startdate =($2) group by link ORDER BY count desc limit 10 ", [ids, cdate])
    let obj = {
        broken_links_404: broken_links_404,
        broken_anchor: broken_anchor,
        total_links: total_links,
        other_issue: other_issue,
        date: date.rows,
        month: month.rows,
        year: year.rows,
        broken_link_table: broken_link_table.rows,
        broken_link_table_2: broken_link_table_2.rows,
    }
    res.send(obj);
})
app.listen(port, err => {
    if (err) {
        console.log('Error in starting node server: ', port);
    } else {
        console.log('Node server started successfully on port: ', port);
        client.connect()
            .then(result => {
                console.log('Connected to database successfully');
            })
            .catch(err => {
                console.log('Error in connecting to database: ', err);
            })
    }
});