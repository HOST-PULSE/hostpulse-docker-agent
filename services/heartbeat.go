package services

import (
	"bytes"
	"fmt"
	"net/http"
	"time"
)

// StartHeartbeatTicker запускает бесконечный цикл отправки пульса каждые 30 секунд.
// Функция экспортируемая (с заглавной буквы), чтобы её можно было вызвать из main.
func StartHeartbeatTicker(url, token string) {
	// Создаем тикер, который срабатывает каждые 30 секунд
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop() // Хорошая практика — освобождать ресурсы тикера

	// Отправляем первый пульс сразу при старте, не дожидаясь таймера
	sendHeartbeat(url, token)

	for range ticker.C {
		sendHeartbeat(url, token)
	}
}

// sendHeartbeat — приватная функция (с маленькой буквы),
// так как она используется только внутри пакета services.
func sendHeartbeat(url, token string) {
	req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte("{}")))
	if err != nil {
		fmt.Printf(" [Heartbeat] Ошибка сборки запроса: %v\n", err)
		return
	}
	req.Header.Set("X-Agent-Type", "docker_agent")
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
