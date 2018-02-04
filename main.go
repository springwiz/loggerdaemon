package main

import "github.com/springwiz/loggerdaemon/input"
import "log"
import "os"
import "github.com/spf13/viper"
import "github.com/allegro/bigcache"
import "time"

func init() {
	// Change the device for logging to stdout
	log.SetOutput(os.Stdout)
}

func main() {
	// read the config yml
	viper.SetConfigName("server")
	viper.AddConfigPath("/Users/sumit/go")
	err := viper.ReadInConfig()
	host := "localhost"
	port := "9999"
	protocol := "tcp"

	if err != nil {
		log.Println("Config file not found...")
		log.Println("Using Defaults")
	}

	host = viper.GetString("server.host")
	port = viper.GetString("server.port")
	protocol = viper.GetString("server.protocol")

	config := bigcache.Config{
		// number of shards (must be a power of 2)
		Shards: viper.GetInt("bigcache.shards"),
		// time after which entry can be evicted
		LifeWindow: viper.GetDuration("bigcache.lifeWindow") * time.Minute,
		// rps * lifeWindow, used only in initial memory allocation
		MaxEntriesInWindow: viper.GetInt("bigcache.maxEntriesInWindow"),
		// max entry size in bytes, used only in initial memory allocation
		MaxEntrySize: viper.GetInt("bigcache.maxEntrySize"),
		// prints information about additional memory allocation
		Verbose: viper.GetBool("bigcache.verbose"),
		// cache will not allocate more memory than this limit, value in MB
		// if value is reached then the oldest entries can be overridden for the new ones
		// 0 value means no size limit
		HardMaxCacheSize: viper.GetInt("bigcache.hardMaxCacheSize"),
		// callback fired when the oldest entry is removed because of its
		// expiration time or no space left for the new entry. Default value is nil which
		// means no callback and it prevents from unwrapping the oldest entry.
		OnRemove: nil,
	}

	cache, initErr := bigcache.NewBigCache(config)
	if initErr != nil {
		log.Fatal(initErr)
	}
	err1 := input.New(host, port, protocol, cache).Run()
	if err1 != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}
