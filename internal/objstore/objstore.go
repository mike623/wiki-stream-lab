// Package objstore is a thin wrapper over the AWS S3 v2 client, pointed at an
// S3-compatible store (RustFS locally). It exists so consumers that archive to
// object storage don't each repeat the endpoint/credential/path-style setup.
package objstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Config is the connection info for the S3-compatible store.
type Config struct {
	Endpoint  string // e.g. http://localhost:9100
	Region    string // any value; S3-compatible stores ignore it but the SDK requires one
	AccessKey string
	SecretKey string
	Bucket    string
}

// Client is a connected, bucket-scoped object store handle.
type Client struct {
	s3     *s3.Client
	bucket string
}

// Open builds the S3 client and ensures the bucket exists. Path-style
// addressing is required: RustFS/MinIO serve buckets as URL paths, not as
// virtual-host subdomains like real AWS.
func Open(ctx context.Context, cfg Config) (*Client, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("objstore: load aws config: %w", err)
	}

	api := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		o.UsePathStyle = true
	})

	c := &Client{s3: api, bucket: cfg.Bucket}
	if err := c.ensureBucket(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

// ensureBucket creates the bucket, treating "already exists" as success so the
// archiver is safe to restart.
func (c *Client) ensureBucket(ctx context.Context) error {
	_, err := c.s3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(c.bucket)})
	if err == nil {
		return nil
	}
	var owned *types.BucketAlreadyOwnedByYou
	var exists *types.BucketAlreadyExists
	if errors.As(err, &owned) || errors.As(err, &exists) {
		return nil
	}
	return fmt.Errorf("objstore: ensure bucket %q: %w", c.bucket, err)
}

// Put writes body at key, overwriting any existing object. Overwrite-on-rewrite
// is what makes at-least-once safe: a replayed batch with the same key lands the
// same bytes instead of duplicating.
func (c *Client) Put(ctx context.Context, key string, body []byte) error {
	_, err := c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})
	if err != nil {
		return fmt.Errorf("objstore: put %q: %w", key, err)
	}
	return nil
}
