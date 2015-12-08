package main

import (
	"fmt"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"html/template"
	"net/http"
)

const rootPage = `
<head>
  <!-- 
       <link href="http://thomasf.github.io/solarized-css/solarized-dark.min.css" rel="stylesheet"></link>
-->
  <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
  <link href="http://thomasf.github.io/solarized-css/solarized-light.min.css" rel="stylesheet"></link>
  <link rel="stylesheet" href="//cdnjs.cloudflare.com/ajax/libs/highlight.js/8.4/styles/default.min.css">
  <script src="//cdnjs.cloudflare.com/ajax/libs/highlight.js/8.4/highlight.min.js"></script>
</head>
<form action="/">
 Year:<br>
  <input type="text" name="year" value="{{.Year}}">
  <br><br>
  <input type="submit" value="Submit">
</form>
<h3>{{.Year}} imdb score distribution</h3>
<meta charset="utf-8">
<style>

.chart div {
  font: 10px sans-serif;
  background-color: steelblue;
  text-align: right;
  padding: 3px;
  margin: 1px;
  color: white;
}

</style>
<body>
<div class="chart"></div>
<script src="//d3js.org/d3.v3.min.js"></script>
<script>

var data = {{.Values}};

var x = d3.scale.linear()
    .domain([0, d3.max(data)])
    .range([0, 420]);

d3.select(".chart")
  .selectAll("div")
    .data(data)
  .enter().append("div")
    .style("width", function(d) { return x(d) + "px"; })
    .text(function(d) { return d; });

</script>
</body>
`

type movie struct {
	Title string
	Year string
	Score float64
}

var db *mgo.Session
var c *mgo.Collection

func rootHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("GET root")
	year := r.FormValue("year")
	dist := getDistInYear(year)
	generatePage(w, dist, year)
}

func getDistInYear(y string) []int {
	retArray := make([]int, 10)
	len, _ := c.Find(bson.M{"year":y}).Count()
	iter := c.Find(bson.M{"year":y}).Iter()
	var m movie
	for i := 0; i < len; i +=1 {
		iter.Next(&m)
		retArray[int(m.Score)] += 1
	}
	return retArray
}

func generatePage(w http.ResponseWriter, dist []int, y string) {
	templ := template.New("root")
	templ, err := templ.Parse(rootPage)
	if err != nil {
		fmt.Println("template hiva")
	}
	templ.Execute(w, struct{
		Values []int
		Year string
	}{dist, y})
}

func initDb() *mgo.Session {
	db, err := mgo.Dial("mongodb:27017")	
	if err != nil {
		panic(err)
	} 
	db.SetMode(mgo.Monotonic, true)
	c = db.DB("test").C("test_2")
	return db
}

func main() {
	db = initDb()
	fmt.Println("connection!!")
	defer db.Close()
	http.HandleFunc("/", rootHandler)
	fmt.Println("set/")
	err := http.ListenAndServe(":9090", nil)
	fmt.Println("lissen...", err)
}

