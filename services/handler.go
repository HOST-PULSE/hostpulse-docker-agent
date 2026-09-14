package services

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)
var (
	processedEvents   = make(map[string]time.Time)
	processedEventsMu sync.Mutex
)
// HandleDockerEvent обрабатывает входящие события Docker, фильтрует их и отправляет алерты.
// (Экспортируемая функция, если выносится в другой пакет — имя изменено на HandleDockerEvent с заглавной буквы)
func HandleDockerEvent(event map[string]interface{}, client *http.Client, alertsURL string, agentToken string) {
	// 1. Фильтруем тип объекта
	typ, _ := event["Type"].(string)
	if typ != "container" {
		return
	}

	// 2. Ловим только действия 'die', 'stop' и 'start'
	action, _ := event["Action"].(string)
	if action != "die" && action != "stop" && action != "start" {
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
		// Если в корне не нашли, берем из Actor.ID
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

	if action == "die" || action == "stop" {
		if shouldSkipEvent(containerID) {
			// Если за последние 2 секунды этот контейнер уже завершал работу — игнорируем дублирующий stop
			return
		}
	}

	if action == "stop" {
		action = "die"
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

	if action == "start" {
		exitCode = "0"
	}

	// Безопасное логирование ID (проверка длины во избежание panic: out of bounds)
	shortID := containerID
	if len(shortID) > 12 {
		shortID = shortID[:12]
	}

	fmt.Printf("\n[🚨 ALERT] Зафиксировано событие '%s' на контейнере: %s (ID: %s, ExitCode: %s)\n",
		action, containerName, shortID, exitCode)

	var logs string
	// Запрашиваем логи контейнера
	if action == "die" {
		logs = getContainerLogs(client, containerID)
	} else if action == "start" {
		logs = "Контейнер успешно запущен в продакшн контуре."
	}

	// Асинхронно отправляем алерт
	go sendAlertService(alertsURL, agentToken, containerName, containerID, exitCode, logs)
}

func sendAlertService(url, token, name, id, exitCode, logs string) {
	payload := map[string]string{
		"container_name": name,
		"container_id":   id,
		"exit_code":      exitCode,
		"logs":           logs,
	}

	// Превращаем map в идеальный валидный JSON-байт-массив
	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		fmt.Printf("Ошибка маршалинга JSON: %v\n", err)
		return
	}

	// Передаем bytes.NewBuffer(jsonBytes) в запрос
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


func shouldSkipEvent(containerID string) bool {
	processedEventsMu.Lock()
	defer processedEventsMu.Unlock()

	now := time.Now()
	// Если событие по этому контейнеру уже было меньше 2 секунд назад — скипаем
	if lastTime, exists := processedEvents[containerID]; exists {
		if now.Sub(lastTime) < 2*time.Second {
			return true
		}
	}

	// Запоминаем текущее время обработки
	processedEvents[containerID] = now
	return false
}