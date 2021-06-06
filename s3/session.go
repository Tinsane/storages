package s3

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
)

// TODO : unit tests
// Given an S3 bucket name, attempt to determine its region
var findBucketRegion = func(bucket string, config *aws.Config) (string, error) {
	input := s3.GetBucketLocationInput{
		Bucket: aws.String(bucket),
	}

	sess, err := session.NewSession(config.WithRegion("us-east-1"))
	if err != nil {
		return "", err
	}

	output, err := s3.New(sess).GetBucketLocation(&input)
	if err != nil {
		return "", err
	}

	if output.LocationConstraint == nil {
		// buckets in "US Standard", a.k.a. us-east-1, are returned as a nil region
		return "us-east-1", nil
	}
	// all other regions are strings
	return *output.LocationConstraint, nil
}

// TODO : unit tests
func GetAWSRegion(s3Bucket string, config *aws.Config, settings map[string]string) (string, error) {
	if region, ok := settings[RegionSetting]; ok {
		return region, nil
	}

	if config.Endpoint == nil ||
		*config.Endpoint == "" {

		if config.Endpoint != nil {
			hostAddr, parseErr := url.Parse(*config.Endpoint)

			if parseErr != nil {
				return "us-east-1", parseErr
			}

			host, _, err := net.SplitHostPort(hostAddr.Host)

			if err != nil {
				return "us-east-1", err
			}

			if strings.HasSuffix(host, ".amazonaws.com") {
				region, err := findBucketRegion(s3Bucket, config)
				return region, errors.Wrapf(err, "%s is not set and s3:GetBucketLocation failed", RegionSetting)
			}

			return "us-east-1", nil
		}

		region, err := findBucketRegion(s3Bucket, config)
		return region, errors.Wrapf(err, "%s is not set and s3:GetBucketLocation failed", RegionSetting)
	} else {
		// For S3 compatible services like Minio, Ceph etc. use `us-east-1` as region
		// ref: https://github.com/minio/cookbook/blob/master/docs/aws-sdk-for-go-with-minio.md
		return "us-east-1", nil
	}
}

func getDefaultConfig(settings map[string]string) *aws.Config {
	config := defaults.Get().Config.
		WithRegion(settings[RegionSetting])

	provider := &credentials.StaticProvider{Value: credentials.Value{
		AccessKeyID:     getFirstSettingOf(settings, []string{AccessKeyIdSetting, AccessKeySetting}),
		SecretAccessKey: getFirstSettingOf(settings, []string{SecretAccessKeySetting, SecretKeySetting}),
		SessionToken:    settings[SessionTokenSetting],
	}}
	providers := make([]credentials.Provider, 0)
	providers = append(providers, provider)
	providers = append(providers, defaults.CredProviders(config, defaults.Handlers())...)
	newCredentials := credentials.NewCredentials(&credentials.ChainProvider{
		VerboseErrors: aws.BoolValue(config.CredentialsChainVerboseErrors),
		Providers:     providers,
	})
	return config.WithCredentials(newCredentials)
}

// TODO : unit tests
func createSession(bucket string, settings map[string]string) (*session.Session, error) {
	config := getDefaultConfig(settings)
	config.MaxRetries = &MaxRetries
	if _, err := config.Credentials.Get(); err != nil {
		return nil, errors.Wrapf(err, "failed to get AWS credentials; please specify %s and %s", AccessKeyIdSetting, SecretAccessKeySetting)
	}

	if endpoint, ok := settings[EndpointSetting]; ok {
		config.Endpoint = aws.String(endpoint)
	}

	if s3ForcePathStyleStr, ok := settings[ForcePathStyleSetting]; ok {
		s3ForcePathStyle, err := strconv.ParseBool(s3ForcePathStyleStr)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse %s", ForcePathStyleSetting)
		}
		config.S3ForcePathStyle = aws.Bool(s3ForcePathStyle)
	}

	region, err := GetAWSRegion(bucket, config, settings)
	if err != nil {
		return nil, err
	}
	config = config.WithRegion(region)

	filePath := settings[s3CertFile]
	if filePath != "" {
		if file, err := os.Open(filePath); err == nil {
			defer file.Close()
			return session.NewSessionWithOptions(session.Options{Config: *config, CustomCABundle: file})
		} else {
			return nil, err
		}
	}

	return session.NewSession(config)
}
