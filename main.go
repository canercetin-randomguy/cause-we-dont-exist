package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/Wissance/stringFormatter"
	"github.com/jackc/pgx/v4"
	"github.com/spf13/viper"
	"log"
	"strings"
	"time"
)

var db *sql.DB
var server string
var port int
var user string
var password string
var database string

func GetCredentials() {
	viper.SetConfigName("credentials")
	viper.SetConfigType("env")
	viper.AddConfigPath(".")

	err := viper.ReadInConfig()

	if err != nil {
		panic(err)
	}

	password = viper.GetString("password")
	user = viper.GetString("user")
	server = viper.GetString("server")
	port = viper.GetInt("port")
	database = viper.GetString("database_name")

}
func TurkishtoEnglish(str string) string {
	start := time.Now()
	valMod := strings.Replace(str, "'", "", -1)
	valMod = strings.Replace(valMod, "Ğ", "G", -1)
	valMod = strings.Replace(valMod, "İ", "I", -1)
	valMod = strings.Replace(valMod, "Ö", "O", -1)
	valMod = strings.Replace(valMod, "Ş", "S", -1)
	valMod = strings.Replace(valMod, "Ü", "U", -1)
	valMod = strings.Replace(valMod, "ç", "c", -1)
	valMod = strings.Replace(valMod, "ğ", "g", -1)
	valMod = strings.Replace(valMod, "ı", "i", -1)
	valMod = strings.Replace(valMod, "ö", "o", -1)
	valMod = strings.Replace(valMod, "ş", "s", -1)
	valMod = strings.Replace(valMod, "ü", "u", -1)
	valMod = strings.Replace(valMod, "Ç", "C", -1)
	fmt.Printf("Time taken during translation of %s: %d\n", str, time.Since(start).Microseconds())
	return valMod
}
func DatabaseCreate() *pgx.Conn {
	GetCredentials()
	options := "&options=--cluster%3Dpool-gorgon-2847"
	dsn := fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=verify-full", user, password, server, port, database)
	dsn = dsn + options
	ctx := context.Background()
	println(dsn)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		log.Fatal("failed to connect database: ", err)
	}

	var now time.Time
	err = conn.QueryRow(ctx, "SELECT NOW()").Scan(&now)
	if err != nil {
		log.Fatal("failed to execute query", err)
	}
	return conn
}
func main() {
	var dataArray [][]string
	dataArray = append(dataArray, []string{"Image", "Title"})
	timenow := time.Now()
	var skip = 40
	var totalProduct = 40000
	var incrementation = 0
	for {
		link := stringFormatter.Format("https://svc-1000-usf.hotyon.com/search?q=&apiKey=ff3d5a12-17b9-4361-825f-30b7ae3854a7&locale=en&collection=210550292631&skip={0}&take=2827&sort=-date", incrementation)
		if incrementation == totalProduct || incrementation > totalProduct-40 {
			break
		}
		RicoScrape("nba", link, dataArray, 1)
		incrementation += skip
	}
	fmt.Println("Time taken during scrape: ", time.Since(timenow))
}

func _() {
	var db = DatabaseCreate()
	var ctx = context.Background()
	err := db.Ping(ctx)
	if err != nil {
		log.Fatal("failed to connect database", err)
	} else {
		fmt.Println("database is... alive.")
	}
}
