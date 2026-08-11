package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
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
	commandURL := djangoURL + "api/v1/agent/commands/"

	agentToken := os.Getenv("HOSTPULSE_TOKEN")
	if agentToken == "" {
		agentToken = "hostpulse_secret_token_123"
	}
	commandPassword := os.Getenv("HOSTPULSE_COMMAND_PASSWORD")

	fmt.Printf(" [INFO] Базовый URL CRM: %s\n", djangoURL)
	fmt.Printf(" Настройки: Отправка алертов на %s\n", alertsURL)
	fmt.Printf(" Настройки: Отправка пульса на %s\n", heartbeatURL)

	go startHeartbeatTicker(heartbeatURL, agentToken)

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
		go startCommandPoller(commandURL, agentToken, commandPassword, client)
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
			handleDockerEvent(event, client, alertsURL, agentToken)
		}

		resp.Body.Close()
		time.Sleep(2 * time.Second)
	}
}

// Выделенный метод обработки, фильтрации и отправки алертов
// Выделенный метод обработки, фильтрации и отправки алертов
func handleDockerEvent(event map[string]interface{}, client *http.Client, alertsURL string, agentToken string) {
	// Оставляем логирование для контроля
	//eventJSON, _ := json.Marshal(event)
	//fmt.Printf("\n[⚙️ DOCKER RAW EVENT]: %s\n", string(eventJSON))

	// 1. Фильтруем тип объекта
	typ, _ := event["Type"].(string)
	if typ != "container" {
		return
	}

	// 2. Ловим только действия 'die' и 'stop'
	action, _ := event["Action"].(string)
	if action != "die" && action != "stop" {
		return
	}

	// 3. УЛЬТРА-НАДЕЖНОЕ ПОЛУЧЕНИЕ ID КОНТЕЙНЕРА
	var containerID string

	// Сначала пробуем взять из корня (маленькими буквами)
	if id, ok := event["id"].(string); ok && id != "" {
		containerID = id
	}

	// Вытягиваем Actor для глубокого разбора
	var actorMap map[string]interface{}
	if actor, ok := event["Actor"].(map[string]interface{}); ok {
		actorMap = actor
		// Если в корне не нашли, берем из Actor.ID (как в вашем логе)
		if containerID == "" {
			if id, ok := actor["ID"].(string); ok {
				containerID = id
			}
		}
	}

	// Если ID так и не нашли, только тогда выходим
	if containerID == "" {
		fmt.Println(" [⚠️ DEBUG] Пропущено событие: не удалось найти ID контейнера")
		return
	}

	containerName := "Неизвестный контейнер"
	exitCode := "1"

	// 4. Безопасно вытягиваем Имя и exitCode из Attributes
	if actorMap != nil {
		if attrs, ok := actorMap["Attributes"].(map[string]interface{}); ok {
			if name, found := attrs["name"].(string); found {
				containerName = name
			}
			if code, found := attrs["exitCode"].(string); found {
				exitCode = code
			}
		}
	}

	// 5. ВЫВОД АЛЕРТА (Теперь он железно сработает)
	fmt.Printf("\n[🚨 ALERT] Зафиксировано событие '%s' на контейнере: %s (ID: %s, ExitCode: %s)\n",
		action, containerName, containerID[:12], exitCode)

	// 6. Запрашиваем логи контейнера
	logs := getContainerLogs(client, containerID)

	// 7. Асинхронно отправляем алерт на Django бэкенд
	go sendAlertService(alertsURL, agentToken, containerName, containerID, exitCode, logs)
}




// Фоновая горутина для регулярной отправки пинга "Я жив"
func startHeartbeatTicker(url, token string) {
	// Создаем тикер, который срабатывает каждые 30 секунд
	ticker := time.NewTicker(30 * time.Second)

	// Отправляем первый пульс сразу при старте, не дожидаясь таймера
	sendHeartbeat(url, token)

	for range ticker.C {
		sendHeartbeat(url, token)
	}
}

func startCommandPoller(commandsURL, token, localPassword string, client *http.Client) {
	// Формируем URL очереди команд
	ticker := time.NewTicker(2 * time.Second)

	for range ticker.C {
		req, _ := http.NewRequest("GET", commandsURL, nil)
		req.Header.Set("X-Agent-Token", token)

		resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
		if err != nil {
			continue // если бэкенд мигнул, просто ждем следующий тик
		}

		if resp.StatusCode == http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			jsonStr := string(bodyBytes)
			commandID := fetchJSONValue(jsonStr, `"command_id":`)
			containerID := fetchJSONValue(jsonStr, `"container_id":`)
			incomingPassword := fetchJSONValue(jsonStr, `"password":`) // <-- Читаем пароль от CRM

			if containerID != "" {
				fmt.Printf(" [COMMAND] Получена команда %s на перезапуск контейнера %s\n", commandID, containerID[:12])

				var success bool

				// ПРОВЕРКА БЕЗОПАСНОСТИ: Сверяем пароль от CRM с локальной переменной
				if incomingPassword != localPassword {
					fmt.Printf(" [⚠️ SECURITY ALERT] Отклонено! Неверный пароль команды для контейнера %s\n", containerID[:12])
					success = false // Блокируем выполнение и ставим статус "провал"
				} else {
					// Если пароли совпали — выполняем перезапуск через Docker сокет
					restartURL := fmt.Sprintf("http://localhost/v1.40/containers/%s/restart", containerID)
					restartReq, _ := http.NewRequest("POST", restartURL, nil)

					restartResp, err := client.Do(restartReq)
					success = err == nil && restartResp.StatusCode == http.StatusNoContent
					if restartResp != nil {
						restartResp.Body.Close()
					}
				}

				// Отчитываемся перед Django, что команда обработана (успешно или заблокирована)
				confirmURL := fmt.Sprintf("%s%s/confirm/", commandsURL, commandID)
				statusPayload := fmt.Sprintf(`{"success": %t}`, success)
				confirmReq, _ := http.NewRequest("POST", confirmURL, bytes.NewBuffer([]byte(statusPayload)))
				confirmReq.Header.Set("X-Agent-Token", token)
				confirmReq.Header.Set("Content-Type", "application/json")

				confirmResp, _ := (&http.Client{Timeout: 5 * time.Second}).Do(confirmReq)
				if confirmResp != nil {
					confirmResp.Body.Close()
				}
				fmt.Println(" [COMMAND] Статус выполнения команды отправлен на бэкенд.")
			}
		} else {
			resp.Body.Close()
		}
	}
}

// Функция отправки HTTP POST запроса пульса
func sendHeartbeat(url, token string) {
	req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte("{}")))
	if err != nil {
		fmt.Printf(" [Heartbeat] Ошибка сборки запроса: %v\n", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Agent-Token", token)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf(" [Heartbeat] Бэкенд недоступен по адресу %s\n", url)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		fmt.Println("💓 [Heartbeat] Пульс успешно доставлен на бэкенд!")
	} else {
		fmt.Printf(" 💓 [Heartbeat] Бэкенд вернул ошибку: %d\n", resp.StatusCode)
	}
}

func getContainerLogs(client *http.Client, id string) string {
	logsURL := fmt.Sprintf("http://localhost/v1.40/containers/%s/logs?stdout=true&stderr=true&tail=50", id)
	resp, err := client.Get(logsURL)
	if err != nil {
		fmt.Printf("Не удалось получить логи: %v\n", err)
		return ""
	}
	defer resp.Body.Close()

	buf := new(bytes.Buffer)
	_, _ = io.Copy(buf, resp.Body)

	return cleanLogs(buf.String())
}

func cleanLogs(raw string) string {
	var cleanLines []string
	lines := strings.Split(raw, "\n")
	for _, line := range lines {
		if len(line) > 8 {
			cleanLines = append(cleanLines, line[8:])
		} else if len(line) > 0 {
			cleanLines = append(cleanLines, line)
		}
	}
	return strings.Join(cleanLines, "\n")
}

func sendAlertService(url, token, name, id, exitCode, logs string) {
	// 1. Создаем структуру (map) с данными
	payload := map[string]string{
		"container_name": name,
		"container_id":   id,
		"exit_code":      exitCode,
		"logs":           logs, // Передаем логи как есть, без ручных замен!
	}

	// 2. Превращаем map в идеальный валидный JSON-байт-массив
	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		fmt.Printf("Ошибка маршалинга JSON: %v\n", err)
		return
	}

	// 3. Передаем bytes.NewBuffer(jsonBytes) в запрос
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBytes))
	if err != nil {
		fmt.Printf("Ошибка создания HTTP запроса: %v\n", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Agent-Token", token)

	defaultClient := &http.Client{Timeout: 10 * time.Second}
	resp, err := defaultClient.Do(req)
	if err != nil {
		fmt.Printf(" Бэкенд недоступен по адресу %s\n", url)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK {
		fmt.Println(" Алерт успешно отправлен на бэкенд CRM!")
	} else {
		fmt.Printf(" Бэкенд вернул ошибку: %d\n", resp.StatusCode)
	}
}

func fetchJSONValue(jsonStr, key string) string {
	idx := strings.Index(jsonStr, key)
	if idx == -1 {
		return ""
	}
	start := idx + len(key)
	var result strings.Builder
	insideQuotes := false
	started := false

	for i := start; i < len(jsonStr); i++ {
		ch := jsonStr[i]
		if ch == '"' {
			if !insideQuotes && !started {
				insideQuotes = true
				started = true
				continue
			}
			if insideQuotes {
				break
			}
		}
		if insideQuotes || (ch >= '0' && ch <= '9') {
			started = true
			result.WriteByte(ch)
		} else if started && (ch == ',' || ch == '}' || ch == ']') {
			break
		}
	}
	return strings.TrimSpace(result.String())
}
