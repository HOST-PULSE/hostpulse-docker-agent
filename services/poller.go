package services

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// DjangoCommandResponse описывает структуру ответа от бэкенда HostPulse
type DjangoCommandResponse struct {
	Command *DockerCommand `json:"command"`
}
type DockerInspectResponse struct {
	State struct {
		Status    string `json:"Status"`
		StartedAt string `json:"StartedAt"`
	} `json:"State"`
}

// DockerListResponse нужен для парсинга базового списка контейнеров
type DockerListResponse struct {
	ID    string   `json:"Id"`
	Names []string `json:"Names"`
}

type ContainerUptimeInfo struct {
	ID        string `json:"container_id"`
	Name      string `json:"name"`
	Status    string `json:"status"`     // например: "running", "exited"
	StartedAt string `json:"started_at"` // ISO дата запуска из Docker
}

// DockerCommand теперь на 100% совпадает с JSON-ответом вашего Django (HostPulsePollCommandsView)
type DockerCommand struct {
	ID          int    `json:"id"`
	ContainerID string `json:"container_id"`
	CommandType string `json:"type"`         // <-- ИСПРАВЛЕНО: теперь строго мапится из ключа "type"
	Password    string `json:"password"`
}

// StartCommandPoller — главный диспетчер (оркестратор) команд
func StartCommandPoller(commandsURL, token, localPassword string, dockerClient *http.Client,listDockerURL string) {

	fmt.Println("🚀 [INIT] Отправка стартового отчета по контейнерам на бэкенд...")
	go handleListContainersWithUptime(dockerClient, listDockerURL, token)

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		req, _ := http.NewRequest("GET", commandsURL, nil)
		req.Header.Set("X-Agent-Token", token)

		resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
		if err != nil {
			continue // Бэкенд мигнул — ждем следующий тик
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			continue
		}

		bodyBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var djangoResp DjangoCommandResponse
		if err := json.Unmarshal(bodyBytes, &djangoResp); err != nil || djangoResp.Command == nil {
			continue // Команд в очереди нет или ошибка парсинга
		}

		cmd := djangoResp.Command

		// ПРОВЕРКА БЕЗОПАСНОСТИ: Пароль проверяем глобально перед любой командой
		if cmd.Password != localPassword {
			fmt.Printf(" ⚠️ [SECURITY ALERT] Неверный пароль для команды %d\n", cmd.ID)
			sendConfirmToDjango(commandsURL, cmd.ID, token, false, "Ошибка безопасности: неверный пароль")
			continue
		}

		var success bool
		var message string

		// --- ДИСПЕТЧЕРИЗАЦИЯ ПО command_type ---
		switch cmd.CommandType {
		case "start", "stop", "restart", "kill":
			// Управление состоянием контейнера
			success, message = handleContainerState(dockerClient, cmd.ContainerID, cmd.CommandType)

		case "list_uptime":
			// Получение списка всех контейнеров
			success, message = handleListContainersWithUptime(dockerClient, listDockerURL, token)

		case "uptime":
			// Получение uptime конкретного контейнера
			success, message = handleContainerUptime(dockerClient, cmd.ContainerID)

		default:
			// Если поле "type" пришло как "restart" или пустое, обрабатываем как рестарт
			if cmd.CommandType == "" || cmd.CommandType == "restart" {
				success, message = handleContainerState(dockerClient, cmd.ContainerID, "restart")
			} else {
				success = false
				message = fmt.Sprintf("Неизвестный тип команды: %s", cmd.CommandType)
				fmt.Printf(" ⚠️ [COMMAND] Получена неизвестная команда: %s\n", cmd.CommandType)
			}
		}

		// Отправляем структурированный отчет на бэкенд
		sendConfirmToDjango(commandsURL, cmd.ID, token, success, message)
	}
}

// 1. Изменение состояния (start/stop/restart)
func handleContainerState(client *http.Client, containerID, action string) (bool, string) {
	if containerID == "" {
		return false, "ID контейнера пустой"
	}

	dockerURL := fmt.Sprintf("http://localhost/containers/%s/%s", containerID, action)
	req, _ := http.NewRequest("POST", dockerURL, nil)
	req.Header.Set("Upgrade", "tcp")

	resp, err := client.Do(req)
	if err != nil {
		return false, fmt.Sprintf("Ошибка сокета Docker: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		shortID := containerID
		if len(shortID) > 8 {
			shortID = shortID[:8]
		}
		fmt.Printf("  [DOCKER] Успешно выполнено действие '%s' для контейнера %s\n", action, shortID)
		return true, fmt.Sprintf("Контейнер %s успешно переведен в статус %s", containerID, action)
	}

	body, _ := io.ReadAll(resp.Body)
	return false, fmt.Sprintf("Docker API вернул ошибку (%d): %s", resp.StatusCode, string(body))
}

// 2. Получение списка контейнеров
func handleListContainers(client *http.Client) (bool, string) {
	dockerURL := "http://localhost/containers/json?all=true"
	resp, err := client.Get(dockerURL)
	if err != nil {
		return false, fmt.Sprintf("Ошибка получения списка контейнеров: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Sprintf("Docker API ошибка списка: %s", string(body))
	}

	fmt.Println("  [DOCKER] Список контейнеров успешно прочитан")
	return true, string(body)
}

// 3. Получение Uptime (Статуса) конкретного контейнера
func handleContainerUptime(client *http.Client, containerID string) (bool, string) {
	if containerID == "" {
		return false, "ID контейнера для проверки uptime не указан"
	}

	dockerURL := fmt.Sprintf("http://localhost/containers/%s/json", containerID)
	resp, err := client.Get(dockerURL)
	if err != nil {
		return false, fmt.Sprintf("Ошибка Inspect контейнера: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Sprintf("Docker Inspect ошибка: %s", string(body))
	}

	shortID := containerID
	if len(shortID) > 8 {
		shortID = shortID[:8]
	}
	fmt.Printf("  [DOCKER] Данные Uptime для %s получены\n", shortID)
	return true, string(body)
}

// Отдельная функция отправки статуса подтверждения на Django
func sendConfirmToDjango(baseCommandsURL string, cmdID int, token string, success bool, msg string) {
	confirmURL := fmt.Sprintf("%s%d/confirm/", baseCommandsURL, cmdID)

	payload := map[string]interface{}{
		"success": success,
		"message": msg,
	}
	jsonBytes, _ := json.Marshal(payload)

	confirmReq, _ := http.NewRequest("POST", confirmURL, bytes.NewBuffer(jsonBytes))
	confirmReq.Header.Set("X-Agent-Token", token)
	confirmReq.Header.Set("Content-Type", "application/json")

	confirmResp, err := (&http.Client{Timeout: 5 * time.Second}).Do(confirmReq)
	if err == nil && confirmResp != nil {
		confirmResp.Body.Close()
	}
	fmt.Printf(" 📤 [COMMAND] Результат выполнения команды %d отправлен на HostPulse.\n", cmdID)
}

func handleListContainersWithUptime(client *http.Client, targetURL string, token string) (bool, string) {
	dockerURL := "http://localhost/containers/json?all=true"
	resp, err := client.Get(dockerURL)
	if err != nil {
		return false, fmt.Sprintf("Ошибка получения списка Docker: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false, fmt.Sprintf("Docker API вернул статус %d при запросе списка", resp.StatusCode)
	}

	var rawContainers []DockerListResponse
	if err := json.NewDecoder(resp.Body).Decode(&rawContainers); err != nil {
		return false, fmt.Sprintf("Ошибка декодирования списка контейнеров: %v", err)
	}

	var resultReport []ContainerUptimeInfo

	for _, c := range rawContainers {
		inspectURL := fmt.Sprintf("http://localhost/containers/%s/json", c.ID)
		inspectResp, err := client.Get(inspectURL)
		if err != nil {
			continue
		}

		var inspectData DockerInspectResponse
		if err := json.NewDecoder(inspectResp.Body).Decode(&inspectData); err == nil {
			containerName := "Неизвестно"
			if len(c.Names) > 0 {
				containerName = c.Names[0]
			}

			resultReport = append(resultReport, ContainerUptimeInfo{
				ID:        c.ID,
				Name:      containerName,
				Status:    inspectData.State.Status,
				StartedAt: inspectData.State.StartedAt,
			})
		}
		inspectResp.Body.Close()
	}

	jsonBytes, _ := json.Marshal(resultReport)
	// Отправляем строго на переданный во внутренний аргумент targetURL
	req, _ := http.NewRequest("POST", targetURL, bytes.NewBuffer(jsonBytes))
	req.Header.Set("X-Agent-Token", token)
	req.Header.Set("Content-Type", "application/json")

	confirmResp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return false, fmt.Sprintf("Не удалось отправить отчет на отдельный эндпоинт: %v", err)
	}
	defer confirmResp.Body.Close()

	if confirmResp.StatusCode == http.StatusOK || confirmResp.StatusCode == http.StatusCreated {
		fmt.Println(" 📊 [UPTIME REPORT] Отчет по контейнерам успешно доставлен в Django!")
		return true, "Отчет успешно сформирован и отправлен"
	}

	return false, fmt.Sprintf("Django эндпоинт вернул ошибку: %d", confirmResp.StatusCode)
}