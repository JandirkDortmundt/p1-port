package main

import (
	"bufio"     // Used for efficient line-by-line reading
	"fmt"       // For printing output
	"io"        // For checking EOF error
	"log"       // For logging status messages and errors
	"os"        // For signal handling and error checking (ErrDeadlineExceeded)
	"os/signal" // For signal handling
	"regexp"    // For regular expression matching to extract values
	"strconv"   // For converting extracted strings to numbers
	"strings"   // For string manipulation (trimming, splitting)
	"syscall"   // For signal handling (SIGTERM)
	"time"      // Used for potential delays

	// Third-party library for serial communication
	"go.bug.st/serial"
)

// --- Configuration ---
// !! IMPORTANT: Replace "/dev/ttyUSB0" with the actual serial port name for your P1 cable on your Raspberry Pi !!
const portName = "/dev/ttyUSB0" // Example: "/dev/ttyUSB0", "/dev/ttyACM0"

// Common P1 port settings (DSMR v5.0 standard).
const (
	baudRate = 115200
	dataBits = 8
	parity   = serial.NoParity
	stopBits = serial.OneStopBit
)

// Interval between the *end* of one successful read cycle and the *start* of the next.
// Set to slightly less than the other app's interval (e.g., 9 seconds if the other app reads every 10s)
// to ensure you get a chance to read, but adjust as needed based on actual timing.
const readInterval = 9 * time.Second

// --- Data Structures ---

// P1Data holds the extracted values from a single telegram.
// We only extract a few key values for this example.
type P1Data struct {
	Timestamp               string  // Telegram timestamp (YYMMDDHHMMSS)
	ElectricityConsumption1 float64 // Meter Reading electricity delivered to client (Tariff 1) in kWh
	ElectricityConsumption2 float64 // Meter Reading electricity delivered to client (Tariff 2) in kWh
	ElectricityProduction1  float64 // Meter Reading electricity delivered by client (Tariff 1) in kWh
	ElectricityProduction2  float64 // Meter Reading electricity delivered by client (Tariff 2) in kWh
	CurrentTariff           int     // Current Tariff indicator (1 or 2)
	CurrentConsumption      float64 // Actual electricity power delivered (+P) in kW
	CurrentProduction       float64 // Actual electricity power received (-P) in kW
	GasTimestamp            string  // Timestamp of gas reading (YYMMDDHHMMSS)
	GasConsumption          float64 // Gas meter reading in m3
}

// --- Regular Expressions ---
// Pre-compile regexps for efficiency. These target specific OBIS codes and capture the value(s).
var (
	regexTimestamp      = regexp.MustCompile(`^0-0:1\.0\.0\(([0-9]{12})[WS]\)`)
	regexElecConsume1   = regexp.MustCompile(`^1-0:1\.8\.1\(([0-9]+\.[0-9]{3})\*kWh\)`)
	regexElecConsume2   = regexp.MustCompile(`^1-0:1\.8\.2\(([0-9]+\.[0-9]{3})\*kWh\)`)
	regexElecProduce1   = regexp.MustCompile(`^1-0:2\.8\.1\(([0-9]+\.[0-9]{3})\*kWh\)`)
	regexElecProduce2   = regexp.MustCompile(`^1-0:2\.8\.2\(([0-9]+\.[0-9]{3})\*kWh\)`)
	regexTariff         = regexp.MustCompile(`^0-0:96\.14\.0\(([0-9]{4})\)`)
	regexCurrentConsume = regexp.MustCompile(`^1-0:1\.7\.0\(([0-9]+\.[0-9]{3})\*kW\)`)
	regexCurrentProduce = regexp.MustCompile(`^1-0:2\.7\.0\(([0-9]+\.[0-9]{3})\*kW\)`)
	regexGas            = regexp.MustCompile(`^0-1:24\.2\.1\(([0-9]{12})[WS]\)\(([0-9]+\.[0-9]{3})\*m3\)`)
)

// --- Main Function ---

func main() {
	log.Println("Starting P1 telegram reader...")

	// --- Signal Handling ---
	// Create a channel to listen for OS signals
	sigChan := make(chan os.Signal, 1)
	// Notify the channel for Interrupt (Ctrl+C) and Terminate signals
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Create a channel to signal the main loop to stop
	done := make(chan struct{})

	// Start a goroutine that waits for a signal and signals the main loop to stop
	go func() {
		<-sigChan // Block until a signal is received
		log.Println("Received interrupt signal. Initiating shutdown...")
		close(done) // Signal the main loop to exit
	}()
	// --- End Signal Handling ---

	// Main loop to repeatedly open, read, close, and wait
	for {
		select {
		case <-done:
			// Received shutdown signal, exit the loop
			log.Println("Shutdown signal received. Exiting program.")
			return // Exit main function
		default:
			// Continue with the read cycle
		}

		log.Printf("Attempting to open serial port %s...", portName)

		mode := &serial.Mode{
			BaudRate: baudRate,
			DataBits: dataBits,
			Parity:   parity,
			StopBits: stopBits,
		}

		port, err := serial.Open(portName, mode)
		if err != nil {
			log.Printf("❌ Failed to open port %s: %v\n"+
				"   Hints:\n"+
				"   1. Is the device name '%s' correct? (Check with 'ls /dev/tty*')\n"+
				"   2. Is the P1 cable plugged in?\n"+
				"   3. Does the user have permission? Try 'sudo usermod -a -G dialout $USER' (and relogin).\n"+
				"   4. Is another program using the port? (Use 'lsof %s' or 'fuser %s')",
				portName, err, portName, portName, portName)
			// If we can't open the port, wait before trying again to avoid spamming
			select {
			case <-time.After(readInterval):
				// Waited for the interval, try again
				continue
			case <-done:
				// Received shutdown signal while waiting
				log.Println("Shutdown signal received while waiting to open port. Exiting program.")
				return
			}
		}

		log.Printf("✅ Successfully opened serial port %s. Reading telegram...", portName)

		reader := bufio.NewReader(port)
		var telegramLines []string // Buffer to hold lines of the current telegram
		readingTelegram := false   // Flag to indicate if we are currently inside a telegram
		telegramComplete := false

		// Loop to read lines until a complete telegram is found or shutdown is signaled
		for !telegramComplete {
			select {
			case <-done:
				// Received shutdown signal, exit the inner read loop
				log.Println("Shutdown signal received while reading telegram. Closing port.")
				port.Close() // Close the port immediately
				return       // Exit main function
			default:
				// Continue reading line
			}

			// Set a read deadline to prevent blocking forever if data stops flowing
			// This is important in an intermittent read scenario.
			// Adjust timeout based on how quickly your meter sends data (usually within a few seconds)
			port.SetReadTimeout(5 * time.Second)

			line, err := reader.ReadString('\n')

			if err != nil {
				if err == io.EOF {
					log.Println("Received EOF while reading telegram. Port likely closed unexpectedly.")
				} else if os.IsTimeout(err) {
					log.Printf("⚠️ Read timeout after 5 seconds while reading telegram. Port may be busy or no data received.")
				} else {
					log.Printf("⚠️ Error reading line from serial port: %v", err)
				}
				// On any read error during telegram assembly, abandon this telegram
				// and close the port to reset.
				port.Close()
				telegramLines = nil     // Clear buffer
				readingTelegram = false // Reset state
				break                   // Exit the inner read loop to try opening the port again
			}

			// Trim whitespace (like CR/LF)
			trimmedLine := strings.TrimSpace(line)

			// --- Telegram Synchronization Logic ---
			if !readingTelegram {
				// We are looking for the start of a new telegram
				if strings.HasPrefix(trimmedLine, "/") {
					log.Println("Found start of telegram ('/'). Beginning to collect lines.")
					readingTelegram = true
					telegramLines = []string{trimmedLine} // Start collecting with the '/' line
				} else {
					// Ignore lines until the start marker is found
					// log.Printf("Ignoring line outside of telegram: %s", trimmedLine) // Optional: uncomment for debugging ignored lines
				}
			} else {
				// We are currently reading inside a telegram
				// Add the line to our buffer if not empty
				if len(trimmedLine) > 0 {
					telegramLines = append(telegramLines, trimmedLine)
				}

				// Check if this line is the end-of-telegram marker '!'
				if strings.HasPrefix(trimmedLine, "!") {
					log.Println("Found end of telegram ('!'). Telegram complete.")
					telegramComplete = true // Mark telegram as complete
				}
			}
			// --- End Synchronization Logic ---
		}

		// --- Process Telegram (if complete) ---
		if telegramComplete && len(telegramLines) > 0 {
			log.Println("Telegram complete. Parsing...")
			parsedData, err := parseTelegram(telegramLines)
			if err != nil {
				log.Printf("⚠️ Error parsing telegram: %v", err)
			} else {
				// Print the parsed data nicely
				fmt.Println("--- Parsed P1 Data ---")
				fmt.Printf("  Timestamp: %s\n", parsedData.Timestamp)
				fmt.Printf("  Electricity Tariff: %d\n", parsedData.CurrentTariff)
				fmt.Printf("  Consumption (T1): %.3f kWh\n", parsedData.ElectricityConsumption1)
				fmt.Printf("  Consumption (T2): %.3f kWh\n", parsedData.ElectricityConsumption2)
				fmt.Printf("  Production  (T1): %.3f kWh\n", parsedData.ElectricityProduction1)
				fmt.Printf("  Production  (T2): %.3f kWh\n", parsedData.ElectricityProduction2)
				fmt.Printf("  Current Consumption: %.3f kW\n", parsedData.CurrentConsumption)
				fmt.Printf("  Current Production:  %.3f kW\n", parsedData.CurrentProduction)
				fmt.Printf("  Gas Timestamp: %s\n", parsedData.GasTimestamp)
				fmt.Printf("  Gas Consumption: %.3f m3\n", parsedData.GasConsumption)
				fmt.Println("----------------------")
			}
		} else if !telegramComplete {
			// If the inner loop broke without completing a telegram (due to read error/timeout)
			log.Println("Telegram read incomplete due to error or timeout.")
		}
		// --- End Process Telegram ---

		// --- Close Port and Wait ---
		log.Println("Closing serial port...")
		if closeErr := port.Close(); closeErr != nil {
			log.Printf("Error closing serial port: %v", closeErr)
		} else {
			log.Println("Serial port closed successfully.")
		}

		log.Printf("Waiting for %s before next read...", readInterval)
		select {
		case <-time.After(readInterval):
			// Waited for the interval, continue loop for next read cycle
		case <-done:
			// Received shutdown signal while waiting
			log.Println("Shutdown signal received while waiting. Exiting program.")
			return // Exit main function
		}
		// --- End Close Port and Wait ---
	}
}

// --- Helper Functions ---

// parseTelegram takes a slice of strings (lines of a P1 telegram)
// and attempts to extract relevant data using regular expressions.
func parseTelegram(lines []string) (P1Data, error) {
	data := P1Data{}
	var parseErrors []string // Collect errors encountered during parsing

	for _, line := range lines {
		// Try matching each line against our pre-compiled regexps
		if match := regexTimestamp.FindStringSubmatch(line); len(match) > 1 {
			data.Timestamp = match[1]
		} else if match := regexElecConsume1.FindStringSubmatch(line); len(match) > 1 {
			// Use blank identifier _ to ignore potential ParseFloat error for brevity
			data.ElectricityConsumption1, _ = strconv.ParseFloat(match[1], 64)
		} else if match := regexElecConsume2.FindStringSubmatch(line); len(match) > 1 {
			data.ElectricityConsumption2, _ = strconv.ParseFloat(match[1], 64)
		} else if match := regexElecProduce1.FindStringSubmatch(line); len(match) > 1 {
			data.ElectricityProduction1, _ = strconv.ParseFloat(match[1], 64)
		} else if match := regexElecProduce2.FindStringSubmatch(line); len(match) > 1 {
			data.ElectricityProduction2, _ = strconv.ParseFloat(match[1], 64)
		} else if match := regexTariff.FindStringSubmatch(line); len(match) > 1 {
			// Use blank identifier _ to ignore potential Atoi error for brevity
			data.CurrentTariff, _ = strconv.Atoi(match[1])
		} else if match := regexCurrentConsume.FindStringSubmatch(line); len(match) > 1 {
			data.CurrentConsumption, _ = strconv.ParseFloat(match[1], 64)
		} else if match := regexCurrentProduce.FindStringSubmatch(line); len(match) > 1 {
			data.CurrentProduction, _ = strconv.ParseFloat(match[1], 64)
		} else if match := regexGas.FindStringSubmatch(line); len(match) > 2 { // Expecting 2 capture groups
			data.GasTimestamp = match[1]
			data.GasConsumption, _ = strconv.ParseFloat(match[2], 64)
		}
		// Add more 'else if' blocks here for other OBIS codes you want to parse
	}

	// Basic validation: Check if at least the main timestamp was found
	if data.Timestamp == "" {
		parseErrors = append(parseErrors, "main timestamp (0-0:1.0.0) not found")
	}
	// You could add more checks here (e.g., was gas data found?)

	if len(parseErrors) > 0 {
		return data, fmt.Errorf("parsing failed for fields: %s", strings.Join(parseErrors, ", "))
	}

	return data, nil
}
