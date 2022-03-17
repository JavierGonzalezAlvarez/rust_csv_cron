use postgres::{Client, Error, NoTls};
use std::collections::HashMap;
use std::process;
use std::time::Instant;

//crontab
use job_scheduler::{Job, JobScheduler};
//use std::time::Duration;

struct DataCsv {
    pub score: String,
    pub pointaverage: String,
}

fn create() -> Result<(), Error> {
    dotenv::dotenv().ok();
    let database = std::env::var("DATABASE").expect("expect connection to DATABASE");
    let mut client = Client::connect(&database, NoTls)?;
    client.batch_execute(
        "
        CREATE TABLE IF NOT EXISTS datacsv (
            id              SERIAL PRIMARY KEY,
            score            VARCHAR NOT NULL,
            pointaverage     VARCHAR NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()            
            )
    ",
    )?;
    Ok(())
}

fn insert(score: &String, pointaverage: &String) -> Result<(), Error> {
    let database = std::env::var("DATABASE").expect("set DATABASE");
    let mut client = Client::connect(&database, NoTls)?;

    println!("3. data inserted - score: {}", score);
    println!("3. data inserted - pointaverage: {}", pointaverage);

    let mut datacsv = HashMap::new();
    datacsv.insert(score, pointaverage);

    for (key, value) in &datacsv {
        let datacsv = DataCsv {
            score: key.to_string(),
            pointaverage: value.to_string(),
        };
        client.execute(
            "INSERT INTO datacsv (score, pointaverage) VALUES ($1, $2)",
            &[&datacsv.score, &datacsv.pointaverage],
        )?;
    }
    Ok(())
}

fn readcsv() -> Result<(), csv::Error> {
    let start = Instant::now();

    let mut reader = csv::Reader::from_path("./data/students.csv")?;
    for result in reader.records() {
        let record = result?;
        println!("1. {:?} - {:?}", record.get(0), record.get(1));

        let (score1, pointaverage1): (Option<&str>, Option<&str>) = (record.get(0), record.get(1));
        println!(
            "2 .Salida without some() => {:?} - {:?}",
            score1.unwrap().to_string(), //print just the value
            pointaverage1.unwrap()       //print just the value
        );

        let score3 = score1.unwrap().to_string(); //convert Option to String
        let pointaverage3 = pointaverage1.unwrap().to_string(); //convert Option to String

        //"Result" needs to have "Err" method
        if let Err(_err) = insert(&score3, &pointaverage3) {
            println!("{:?}", _err);
            process::exit(1);
        }
    }

    //duration of the csv function
    let duration = start.elapsed();
    println!("It took ...: {:?} milliseconds", duration);

    Ok(())
}

fn main() {
    let mut cron = JobScheduler::new();
    cron.add(Job::new("0 1/1 * * * *".parse().unwrap(), || {
        println!("I get executed every 1 minute!");
        println!("------------------------------");
        println!("sec   min   hour   day of month   month   day of week   year");
        println!(" *     *     *        *              *        *           *");

        if let Err(err) = create() {
            println!("Error: {:?}", err);
            process::exit(1);
        }
        if let Err(err) = readcsv() {
            println!("Error: {:?}", err);
            process::exit(1);
        }
    }));

    loop {
        cron.tick();
    }
}
