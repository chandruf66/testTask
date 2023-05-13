const express = require('express')
const bodyparser = require('body-parser')
const fs = require('fs');
const path = require('path')
const mysql = require('mysql')
const multer = require('multer')
const csv = require('fast-csv');
const { log } = require('console');
 
const app = express()
app.use(express.static("./public"))
 
app.use(bodyparser.json())
app.use(bodyparser.urlencoded({
    extended: false
}))
 
// Database connection
const db = mysql.createConnection({
    host: "localhost",
    user: "root",
    password: "",
    database: "backend"
})
 
db.connect(function (err) {
    if (err) {
        return console.error(err.message);
    }
    console.log('Connected to database.');
})
 
var storage = multer.diskStorage({
    destination: (req, file, callBack) => {
        callBack(null, './uploads/')    
    },
    filename: (req, file, callBack) => {
      global.fname=file.originalname;
        callBack(null, file.fieldname + '-' + Date.now() + path.extname(file.originalname))
    }
})
 
var upload = multer({
    storage: storage
});
 
app.get('/', (req, res) => {
    
  res.sendFile(__dirname + '/index.html');
  
});
 
app.post('/import-csv', upload.single("import-csv"), (req, res) =>{
    console.log("********");
    uploadCsv(__dirname + '/uploads/' + req.file.filename);
   
    console.log('File has imported :');
});

app.post('/api/v1/new-movie',(req,res)=>{
    const tconst=req.body.tconst;
    const titleType=req.body.titleType;
    const runtimeMinutes=req.body.runtimeMinutes;
    const primaryTitle=req.body.primaryTitle;
    const genres=req.body.genres;
    db.query('insert into movies(tconst,titleType,runtimeMinutes,primaryTitle,genres) values(?,?,?,?,?)',[tconst,titleType,runtimeMinutes,primaryTitle,genres],(err,res1)=>{
      if(res1){
        res.send("successs")
      }
      else{
        console.log(err);
      }
    });

});



app.post('/api/v1/update-runtime-minutes',(req,res)=>{

  let query='UPDATE movies  SET runtimeMinutes = CASE genres WHEN "Documentary" THEN runtimeMinutes+15  WHEN "Animation" THEN runtimeMinutes+30 ELSE runtimeMinutes+45  END';
  db.query(query,(error, res1) => {
    if(res){
      res.send("success")
    }
    else{
      res.send(error)
    }
    
    });

});




 
app.get('/api/v1/longest-duration-movies', (req, res) => {
  
  let query = 'select * from movies ORDER BY runtimeMinutes DESC LIMIT 5';
  db.query(query,(error, res1) => {
  res.send(res1)
  });
});

app.get('/api/v1/top-rated-movies', (req, res) => {
  
  let query = 'select * from ratings where averageRating>6.0';
  db.query(query,(error,res1) => {
    res.send(res1)
});
    
     

});

app.get('/api/v1/genre-movies-with-subtotals', (req, res) => {
  
     let query ='SELECT movies.genres, movies.primaryTitle, SUM(ratings.numVotes) as numVotes FROM movies, ratings WHERE movies.tconst = ratings.tconst GROUP BY movies.genres';
     db.query(query,(error, res1) => {
       res.send(res1)
       });
      
    });

function uploadCsv(uriFile){
 
    let stream = fs.createReadStream(uriFile);
    let csvDataColl = [];
    let fileStream = csv
        .parse()
        .on("data", function (data) {
            csvDataColl.push(data);
         
        })
        .on("end", function () {
            csvDataColl.shift();
            console.log(fname);
           
           if(fname=="movies.csv"){
                    console.log("movies");
                    let query = 'INSERT INTO movies (tconst,titleType,primaryTitle,runtimeMinutes,genres) VALUES ?';
                    db.query(query, [csvDataColl], (error, res) => {
                        console.log(error || res);
                        if(res){
                          console.log("---")
                        }
                    });
                  
                  }
                  else if(fname=="ratings.csv"){
                    console.log("ratings");
                    let query = 'INSERT INTO ratings (tconst,averageRating,numVotes) VALUES ?';
                    db.query(query, [csvDataColl], (error, res) => {
                        console.log(error || res);
                        if(res){
                        
                        }
                    });
                  }
            
             
            fs.unlinkSync(uriFile)
        });
  
    stream.pipe(fileStream);
}



 
app.listen(5000,()=>{
    console.log("5000");
})