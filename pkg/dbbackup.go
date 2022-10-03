package pkg

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sort"
	"time"

	"github.com/kelseyhightower/envconfig"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type DBBackupConfig = struct {
	// postgres
	DbName           string `default:"postgres" split_words:"true"`
	DbHost           string `default:"localhost" split_words:"true"`
	DbPort           int    `default:"5432" split_words:"true"`
	DbUsername       string `default:"postgres" split_words:"true"`
	DbPassword       string `required:"true" split_words:"true"`
	DbBackupFormat   string `default:"t" split_words:"true"`
	DbBackupFilePath string `defualt:"/tmp" split_words:"true"`
	DbBackupFileName string `required:"true" split_words:"true"`
	DbMaxBackups     int    `default:"10" split_words:"true"`

	// minio
	MinioEndpoint        string `required:"true" split_words:"true"`
	MinioAccessKeyId     string `required:"true" split_words:"true"`
	MinioAccessKeySecret string `required:"true" split_words:"true"`
	MinioBucketLocation  string `required:"true" split_words:"true"`
	MinioBucketName      string `required:"true" split_words:"true"`
}

func DBBackup(ctx context.Context) {
	// Parse config
	var cfg DBBackupConfig
	if err := envconfig.Process("dbsidekick", &cfg); err != nil {
		log.Fatalln(err)
	}

	// Create Postgres backup
	backupFilename := fmt.Sprintf("%s_%d.dump", cfg.DbBackupFileName, time.Now().UnixMilli())
	backupFQDN := fmt.Sprintf("/tmp/%s", backupFilename)

	var pgDumpOptions = []string{
		"-Fc",
		"-Z",
		"9",
		fmt.Sprintf("--dbname=%s", cfg.DbName),
		fmt.Sprintf("--host=%s", cfg.DbHost),
		fmt.Sprintf("--port=%d", cfg.DbPort),
		fmt.Sprintf("--username=%s", cfg.DbUsername),
		fmt.Sprintf(`--file=%s`, backupFQDN),
	}

	cmd := exec.Command("pg_dump", pgDumpOptions...)
	cmd.Env = append(os.Environ(), fmt.Sprintf(`PGPASSWORD=%v`, cfg.DbPassword))

	var output string
	stderrIn, _ := cmd.StderrPipe()
	go func() {
		output = streamExecOutput(stderrIn, ExecOptions{StreamPrint: false})
	}()

	cmd.Start()
	err := cmd.Wait()
	if exitError, ok := err.(*exec.ExitError); ok {
		log.Fatalln(output)
		log.Fatalln(exitError)
	}

	// Upload the backup to MinIO
	minioClient, err := minio.New(cfg.MinioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinioAccessKeyId, cfg.MinioAccessKeySecret, ""),
		Secure: true,
	})
	if err != nil {
		log.Fatalln(err)
	}

	// Create bucket if it does not exist
	err = minioClient.MakeBucket(ctx, cfg.MinioBucketName, minio.MakeBucketOptions{Region: cfg.MinioBucketLocation})
	if err != nil {
		_, errBucketExists := minioClient.BucketExists(ctx, cfg.MinioBucketName)
		if errBucketExists != nil {
			log.Fatalln(err)
		}
	}

	var objectArray []minio.ObjectInfo
	objectCh := minioClient.ListObjects(ctx, cfg.MinioBucketName, minio.ListObjectsOptions{
		WithMetadata: true,
	})
	for object := range objectCh {
		if object.Err == nil {
			objectArray = append(objectArray, object)
		}
	}

	// Sort descendingly by last modified
	sort.Slice(objectArray, func(i, j int) bool {
		return objectArray[i].LastModified.After(objectArray[j].LastModified)
	})

	// Delete old backups if necessary
	for i, o := range objectArray {
		if i >= cfg.DbMaxBackups-1 {
			err = minioClient.RemoveObject(ctx, cfg.MinioBucketName, o.Key, minio.RemoveObjectOptions{
				GovernanceBypass: true,
			})
			if err != nil {
				log.Fatalln(err)
			}

			log.Printf("Deleted old backup %s\n", o.Key)
		}
	}

	// Upload the backup
	info, err := minioClient.FPutObject(
		ctx,
		cfg.MinioBucketName,
		backupFilename,
		backupFQDN,
		minio.PutObjectOptions{ContentType: "application/text"},
	)
	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("Successfully uploaded %s of size %d\n", backupFilename, info.Size)
	if err := os.Remove(backupFQDN); err != nil {
		log.Printf("Error: Failed to delete %s locally\n", backupFQDN)
	}
}
