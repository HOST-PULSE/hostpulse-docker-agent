package main

import (

	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"

	"net/http"
	"os"
	
	"strings"
	"docker-agent/services"
	"time"
)

func main() {
	fmt.Println("=== Агент HostPulse успешно запущен и слушает Docker Events ===")

	djangoURL := os.Getenv("HOSTPULSE_URL")
	if djangoURL == "" {
		djangoURL = "https://zedform.kz"
	}

	if !strings.HasSuffix(djangoURL, "/") {
		djangoURL += "/"
	}

	alertsURL := djangoURL + "api/v1/alerts/"
	heartbeatURL := djangoURL + "api/v1/heartbeat/"
	commandURL := djangoURL + "api/v1/commands/"
	listDockerURL := djangoURL + "api/v1/containers-uptime/"

	agentToken := os.Getenv("HOSTPULSE_TOKEN")
	if agentToken == "" {
		agentToken = "hostpulse_secret_token_123"
	}
	commandPassword := os.Getenv("HOSTPULSE_SECRET")

	fmt.Printf(" [INFO] Базовый URL CRM: %s\n", djangoURL)
	fmt.Printf(" Настройки: Отправка алертов на %s\n", alertsURL)
	fmt.Printf(" Настройки: Отправка пульса на %s\n", heartbeatURL)
	fmt.Printf(" Настройки: Отправка команд на %s\n", commandURL)

	go services.StartHeartbeatTicker(heartbeatURL, agentToken)

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", "/var/run/docker.sock")
			},
		},
		Timeout: 0,
	}

	if commandPassword != "" {
		fmt.Println(" [SECURITY] Переменная HOSTPULSE_COMMAND_PASSWORD найдена. Поллер команд успешно активирован.")
		go services.StartCommandPoller(commandURL, agentToken, commandPassword, client, listDockerURL)
	} else {
		fmt.Println(" [⚠️ SECURITY WARNING] Переменная HOSTPULSE_COMMAND_PASSWORD пуста! Поллер удаленных команд отключен в целях безопасности.")
	}

	// Бесконечный цикл верхнего уровня для автоматического ПЕРЕПОДКЛЮЧЕНИЯ
	for {
		fmt.Println(" [INFO] Подключение к Docker сокету для прослушивания событий...")

		eventsURL := "http://localhost/events"
		resp, err := client.Get(eventsURL)
		if err != nil {
			fmt.Printf(" [❌ ERROR] Ошибка подключения к Docker сокету: %v. Повтор через 5 секунд...\n", err)
			time.Sleep(5 * time.Second)
			continue
		}

		decoder := json.NewDecoder(resp.Body)

		// ВНУТРЕННИЙ ЦИКЛ: Читаем бесконечный поток событий из сокета
		for {
			var event map[string]interface{}

			if err := decoder.Decode(&event); err != nil {
				if err == io.EOF {
					fmt.Println(" [⚠️ INFO] Стрим событий Docker завершился (EOF). Переподключение...")
				} else {
					fmt.Printf(" [❌ ERROR] Ошибка декодирования события: %v\n", err)
				}
				break
			}

			// Вызываем наш выделенный метод обработки события
			services.HandleDockerEvent(event, client, alertsURL, agentToken)
		}

		resp.Body.Close()
		time.Sleep(2 * time.Second)
	}
}




//
//func cleanLogs(raw string) string {
//	var cleanLines []string
//	lines := strings.Split(raw, "\n")
//	for _, line := range lines {
//		if len(line) > 8 {
//			cleanLines = append(cleanLines, line[8:])
//		} else if len(line) > 0 {
//			cleanLines = append(cleanLines, line)
//		}
//	}
//	return strings.Join(cleanLines, "\n")
//}
//
//
//
//func fetchJSONValue(jsonStr, key string) string {
//	idx := strings.Index(jsonStr, key)
//	if idx == -1 {
//		return ""
//	}
//	start := idx + len(key)
//	var result strings.Builder
//	insideQuotes := false
//	started := false
//
//	for i := start; i < len(jsonStr); i++ {
//		ch := jsonStr[i]
//		if ch == '"' {
//			if !insideQuotes && !started {
//				insideQuotes = true
//				started = true
//				continue
//			}
//			if insideQuotes {
//				break
//			}
//		}
//		if insideQuotes || (ch >= '0' && ch <= '9') {
//			started = true
//			result.WriteByte(ch)
//		} else if started && (ch == ',' || ch == '}' || ch == ']') {
//			break
//		}
//	}
//	return strings.TrimSpace(result.String())
//}
