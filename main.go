package main

import (
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	done := make(chan struct{})

	var producerWg sync.WaitGroup
	var consumerWg sync.WaitGroup
	weatherChan := make(chan WeatherUpdate)
	producerWg.Go(
		func() {
			insertWeatherData(done, weatherChan)
		},
	)

	consumerWg.Go(
		func() {
			consumeWeatherData(weatherChan)
		},
	)

	<-sigChan
	fmt.Println("\nreceived interrupt signal, shutting down...")
	close(done)

	producerWg.Wait()
	close(weatherChan)

	consumerWg.Wait()
	fmt.Println("All weather data processed")
}

type WeatherUpdate struct {
	Weather Weather
	Index   int
	Time    time.Time
}

func insertWeatherData(done <-chan struct{}, ch chan<- WeatherUpdate) {
	data := []byte{}
	wp := newWeatherPacker(data)
	i := 0

	for {
		select {
		case <-done:
			fmt.Printf("\nProducer stopping... Total entries: %d, Data size: %d bytes\n", i, len(wp.data))
			return
		default:
			requiredBytes := ((i+1)*2 + 7) / 8
			if requiredBytes > len(wp.data) {
				newData := make([]byte, requiredBytes)
				copy(newData, wp.data)
				wp.data = newData
			}

			w := rand.IntN(4)
			wp.Set(Weather(w), i)

			ch <- WeatherUpdate{
				Weather: Weather(w),
				Index:   i,
				Time:    time.Now(),
			}
			i++
			time.Sleep(1 * time.Second)
		}
	}

}

func consumeWeatherData(ch <-chan WeatherUpdate) {
	for update := range ch {
		fmt.Printf("[%s] Index %d: %s\n",
			update.Time.Format("15:04:05.000"),
			update.Index,
			update.Weather.String())
	}
}

type Weather uint8

const (
	Sunny Weather = iota
	Overcast
	Rainy
	Windy
)

func (w Weather) String() string {
	switch w {
	case Sunny:
		return "Sunny"
	case Overcast:
		return "Overcast"
	case Rainy:
		return "Rainy"
	case Windy:
		return "Windy"
	default:
		return "Unknown"
	}
}

type WeatherPacker struct {
	data []byte
}

func newWeatherPacker(data []byte) *WeatherPacker {
	wp := &WeatherPacker{
		data: data,
	}

	return wp
}

func (wp *WeatherPacker) Set(weather Weather, index int) {
	bitPos := index * 2
	byteIndex := bitPos / 8
	bitOffset := bitPos % 8

	wp.data[byteIndex] = (wp.data[byteIndex] &^ (0x3 << bitOffset)) | (uint8(weather) << uint8(bitOffset))
}

func (wp *WeatherPacker) Get(index int) Weather {
	bitPost := index * 2
	byteIndex := bitPost / 8
	bitOffset := bitPost % 8

	return Weather((wp.data[byteIndex] >> bitOffset) & 0x3)
}
