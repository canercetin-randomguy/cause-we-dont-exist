package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"github.com/jackc/pgtype"
	"io"
	"io/fs"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

var httpClient = http.DefaultClient

type RicoINC struct {
	Data struct {
		Query            string      `json:"query"`
		TotalCollections int         `json:"totalCollections"`
		Collections      interface{} `json:"collections"`
		TotalPages       int         `json:"totalPages"`
		Pages            interface{} `json:"pages"`
		Suggestions      interface{} `json:"suggestions"`
		Total            int         `json:"total"`
		Items            []struct {
			ID          int64     `json:"id"`
			ProductType string    `json:"productType"`
			Title       string    `json:"title"`
			Description string    `json:"description"`
			Collections []int64   `json:"collections"`
			Tags        []string  `json:"tags"`
			URLName     string    `json:"urlName"`
			Vendor      string    `json:"vendor"`
			Date        time.Time `json:"date"`
			Variants    []struct {
				ID             int64         `json:"id"`
				Sku            string        `json:"sku"`
				Barcode        string        `json:"barcode"`
				Available      int           `json:"available"`
				Price          float64       `json:"price"`
				Weight         float64       `json:"weight"`
				CompareAtPrice int           `json:"compareAtPrice"`
				ImageIndex     int           `json:"imageIndex"`
				Options        []interface{} `json:"options"`
				Metafields     interface{}   `json:"metafields"`
				Flags          int           `json:"flags"`
			} `json:"variants"`
			SelectedVariantID interface{} `json:"selectedVariantId"`
			Images            []struct {
				URL    string `json:"url"`
				Alt    string `json:"alt"`
				Width  int    `json:"width"`
				Height int    `json:"height"`
			} `json:"images"`
			Metafields  []interface{} `json:"metafields"`
			Options     []interface{} `json:"options"`
			Review      int           `json:"review"`
			ReviewCount int           `json:"reviewCount"`
			Extra       interface{}   `json:"extra"`
		} `json:"items"`
		Facets []struct {
			ID          int    `json:"id"`
			Title       string `json:"title"`
			FacetName   string `json:"facetName"`
			LabelPrefix string `json:"labelPrefix"`
			Multiple    int    `json:"multiple"`
			Display     string `json:"display"`
			Sort        int    `json:"sort"`
			MaxHeight   string `json:"maxHeight"`
			Labels      []struct {
				Label string `json:"label"`
				Value int    `json:"value"`
			} `json:"labels"`
		} `json:"facets"`
		Extra struct {
			Collections []struct {
				ID          int64  `json:"id"`
				Title       string `json:"title"`
				URLName     string `json:"urlName"`
				Description string `json:"description"`
				ImageURL    string `json:"imageUrl"`
				SortOrder   string `json:"sortOrder"`
			} `json:"collections"`
		} `json:"extra"`
	} `json:"data"`
}

func RicoScrape(basename string, link string, dataArray [][]string, option int) int {
	var products RicoINC
	var image string
	var title string
	// Append dummy value to dataArray
	req, err := http.NewRequest("GET", link, nil)
	resp, err := httpClient.Do(req)
	if err != nil {
		if resp.StatusCode == http.StatusNotFound {
			return resp.StatusCode
		}
	}
	if resp.StatusCode != 200 {
		return resp.StatusCode
	}
	body, err := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(body, &products)
	if err != nil {
		panic(err)
	}
	for v := range products.Data.Items {
		if option == 1 {
			title = products.Data.Items[v].Title
		} else if option == 2 {
			title = products.Data.Items[v].Title
		} else {
			title = products.Data.Items[v].Title
		}
		for i := range products.Data.Items[v].Images {
			image = products.Data.Items[v].Images[i].URL
			break
		}
		dataArray = append(dataArray, []string{image, title})
	}
	// Write dataArray to a csv file
	fileName := basename + ".csv"
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	csvWriter := csv.NewWriter(file)
	for rows := range dataArray {
		err := csvWriter.Write(dataArray[rows])
		if err != nil {
			panic(err)
		}
	}
	return 0
}

// This will get called for each HTML element found
func findLinks(_ int, element *goquery.Selection) {
	var href string
	// See if the href attribute exists on the element
	exists := element.HasClass("container container--flush")
	if exists {
		linkStr := href
		// Append linkStr to access.txt file
		file, err := os.OpenFile("access.txt", os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return
		}
		defer func(file *os.File) {
			err := file.Close()
			if err != nil {
				return
			}
		}(file)
		if strings.Contains(linkStr, "/product/") {
			fmt.Println("Product link found: " + linkStr)
			_, err := file.WriteString(linkStr + "\n")
			if err != nil {
				return
			}
		}
	}
}

// processImages will process each image found in the HTML document. Best used with test.Find("img").Each(processImages)
func processImages(index int, element *goquery.Selection) {
	// See if the href attribute exists on the element
	href, exists := element.Attr("src")
	if exists {
		fmt.Println(href)
		err := ioutil.WriteFile("./scraping.txt", []byte(href), fs.ModeAppend)
		if err != nil {
			return
		}
	}
}

//Cookie creates and returns a new cookie to add to the request.
func Cookie(name string, value string) http.Cookie {
	var cookieJar = http.Cookie{Name: name, Value: value}
	return cookieJar
}

//
// linkFetcher scans access.txt file and fetches each link.
//
func linkFetcher() []string {
	var links []string
	f, err := os.OpenFile("access.txt", os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}
	reader := bufio.NewScanner(f)
	for reader.Scan() {
		links = append(links, reader.Text())
	}
	return links
}

//
//linkFind will find all links in the HTML document specified with main parameter, saves them to a file and cleanups the duplicates.
//
func linkFind(main string) {
	var mainLink = main
	req, err := http.NewRequest("GET", mainLink, nil)
	if err != nil {
		panic(err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36")
	resp, err := httpClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			panic(err)
		}
	}(resp.Body)
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		panic(err)
	}
	doc.Find("a").Each(findLinks)
	DuplicateTXTCleanup()
}

// LinkFindScroll will scroll down the page and find all available links.
//
// For example,https://www.trendyol.com/sr?mid=104889&wc=82%2C114&sst=BEST_SELLER&pi=2, where &pi=2 is the scroll down pagination.
// So query parameter = pi, and cnt is starting from 2.
//
// Main link parameter is the https://www.trendyol.com/sr?mid=104889&wc=82%2C114&sst=BEST_SELLER with pagination query trimmed.
//
func LinkFindScroll(mainLink string, query string, cnt int) {
	queryPagination := fmt.Sprintf("%s/%s/%d", mainLink, query, cnt)
	req, err := http.NewRequest("GET", queryPagination, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
	req.Header.Set("Cookie", "keep_alive=8be42360-bb4d-4080-b07f-41de55e84eda; secure_customer_sig=; localization=US; cart_currency=USD; _orig_referrer=https%3A%2F%2Fwww.google.com%2F; _landing_page=%2F; _y=a29549ac-034f-4e1c-b6dd-4ff4cd3dbaac; _s=8be42360-bb4d-4080-b07f-41de55e84eda; _shopify_y=a29549ac-034f-4e1c-b6dd-4ff4cd3dbaac; _shopify_s=8be42360-bb4d-4080-b07f-41de55e84eda; _secure_session_id=fe106eb163915c86c1bcd36bd11efb0e; _shopify_sa_p=; _shopify_sa_t=2022-08-14T11%3A57%3A58.098Z")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
	req.Header.Set("Accept-Encoding", "gzip, deflate, sdch")
	req.Header.Set("Accept-Language", "en-US,en;q=0.8")
	req.Header.Set("Cache-Control", "max-age=0")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Upgrade-Insecure-Requests", "1")
	if err != nil {
		panic(err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		panic(err)
	}
	if resp.StatusCode == 200 {
		linkFind(queryPagination)

	} else {
		fmt.Println("Finished fetching> ", mainLink)
		return
	}
	LinkFindScroll(mainLink, query, cnt+1)
}
func ScrapetoDatabase(db *pgx.Conn, schema string, table string) []map[string]any { // Main scraping code
	var itemPrices = make(map[string]pgtype.Float8)
	var rowCSTR string
	var storageData string
	var tempStorage []map[string]any
	// Append a dummy value to tempStorage, for every element type there must be a dummy value in the array.
	tempStorage = append(tempStorage, map[string]any{"": ""})
	tempStorage = append(tempStorage, map[string]any{"": ""})
	// Read links in access.txt.
	links := linkFetcher()
	for _, link := range links {
		if strings.Contains(link, "(0)") { // Avoid javascript:void(0)
			continue
		} else {
			// Fetch the row count of the table, for synchronization purposes.
			rowC := rowCount(db, schema, table, "Tarih")
			// Classic for loop, for every link in the array, fetch the data.
			req, err := http.NewRequest("GET", link, nil)
			resp, err := httpClient.Do(req)
			if err != nil { // If request is not successful, log the error and continue.
				logFile, err := os.OpenFile("error.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					panic(err)
				}
				errMessage := fmt.Sprintf("Error running request on the link: %s\n", link)
				_, err = logFile.WriteString(errMessage)
				if err != nil {
					panic(err)
				}
			} else { // Else, do something with the response.
				defer func(Body io.ReadCloser) {
					err := Body.Close()
					if err != nil {
						panic(err)
					}
				}(resp.Body)
				body, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					panic(err)
				}
				document, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
				if err != nil {
					panic(err)
				}
				// First column and the element should be the anchor point for syncing the data.
				// and added to database with AddData(db, tempStorage[0][storageData], schema, table, "Tarih").
				//
				// Second column and the rest should be added with
				// UpdateData(db, "TestSchema", "TestTable", tempStorage[x][storageData], "Baslik", "Tarih", tempStorage[0]["Tarih"+"_"+rowCSTR])
				// Vars may differ.
				document.Find("img").Each(func(index int, element *goquery.Selection) {
					exists := element.HasClass("wp-post-image")
					if exists {
						if element.HasClass("woocommerce-placeholder wp-post-image") {
							fmt.Println("No image")
						} else {
							href, exists := element.Attr("src")
							href = strings.Replace(href, "-600x600.jpg", ".jpg", -1)
							if exists {
								rowCSTR = strconv.Itoa(rowC)
								storageData = "Image" + "_" + rowCSTR
								tempStorage[0][storageData] = href
								AddData(db, tempStorage[0][storageData], schema, table, "image")
								for k, v := range itemPrices {
									if strings.Contains(href, k) {
										UpdateDataFloat(db, schema, table, v.Float, "price", "image", tempStorage[0]["Image"+"_"+rowCSTR])
										break
									}
								}
							}
						}
					}
				})
				document.Find("h1").Each(func(index int, element *goquery.Selection) {
					exists := element.HasClass("product_title entry-title")
					if exists {
						rowCSTR = strconv.Itoa(rowC)
						storageData = "Title_" + rowCSTR
						tempStorage[1][storageData] = element.Text()
						UpdateData(db, schema, table, element.Text(), "title", "image", tempStorage[0]["Image"+"_"+rowCSTR])
					}
				})
			}
		}
	}
	DatabaseCleanup(db, "testschema", "productswithprices", "title")
	return tempStorage
}
