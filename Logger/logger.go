package Logger

import "github.com/lni/dragonboat/v3/logger"

var Logger logger.ILogger

func init() {
	Logger = logger.GetLogger("Logger")
	Logger.SetLevel(logger.INFO)
}