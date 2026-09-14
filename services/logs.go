package services

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
)

// getContainerLogs — приватная функция для пакета services.
// Из handler.go она вызывается просто как getContainerLogs(client, id).
func getContainerLogs(client *http.Client, id string) string {
	// Универсальный URL БЕЗ указания версии (работает на v1.40, v1.44, v1.46+)
	logsURL := fmt.Sprintf("http://localhost/containers/%s/logs?stdout=true&stderr=true&tail=50", id)

	resp, err := client.Get(logsURL)
	if err != nil {
		fmt.Printf("Не удалось получить логи: %v\n", err)
		return ""
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Если Docker вернул ошибку, прочитаем её, чтобы понять причину
		errBuf := new(bytes.Buffer)
		_, _ = io.Copy(errBuf, resp.Body)
		fmt.Printf("Docker API вернул ошибку %d: %s\n", resp.StatusCode, errBuf.String())
		return ""
	}

	var resultBuffer bytes.Buffer
	header := make([]byte, 8) // Буфер для 8-байтового multiplex заголовка Docker

	for {
		// 1. Читаем ровно 8 байт заголовка фрейма
		_, err := io.ReadFull(resp.Body, header)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break // Логи успешно прочитаны до конца
		}
		if err != nil {
			fmt.Printf("Ошибка чтения заголовка Docker логов: %v\n", err)
			break
		}

		// 2. Извлекаем длину текстового фрейма из последних 4 байт заголовка (BigEndian)
		frameSize := binary.BigEndian.Uint32(header[4:8])

		// 3. Читаем саму строку лога строго по вычисленной длине frameSize
		frameBuffer := make([]byte, frameSize)
		_, err = io.ReadFull(resp.Body, frameBuffer)
		if err != nil {
			fmt.Printf("Ошибка чтения тела фрейма логов: %v\n", err)
			break
		}

		// Записываем чистую строку в итоговый буфер
		resultBuffer.Write(frameBuffer)
	}

	return resultBuffer.String()
}
