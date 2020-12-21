package storage

import (
	"bytes"
	"context"
	"io/ioutil"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	DefaultRoot = "/"
)

// S3Storage implements the Storage interface for interacting with AWS S3.
type S3Storage struct {
	Bucket  string
	Root    string
	Session *session.Session
}

// NewS3Storage creates a new S3Storage with a new aws.Session.
func NewS3Storage(bucket string) S3Storage {
	cfg := session.New(aws.NewConfig())

	return S3Storage{
		Bucket:  bucket,
		Root:    DefaultRoot,
		Session: cfg,
	}
}

// Write writes the data to the key in the S3 Bucket, with Options applied.
func (s S3Storage) Write(ctx context.Context, key string, body []byte, options *Options) error {
	svc := s3.New(s.Session)

	poi := s3.PutObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	}

	if options != nil {
		if options.TTL > 0 {
			expiry := time.Now().Add(time.Duration(options.TTL) * time.Second)
			poi.Expires = &expiry
		}
	}

	if _, err := svc.PutObject(&poi); err != nil {
		return err
	}

	return nil
}

// Read will read the data from the S3 Bucket.
func (s S3Storage) Read(ctx context.Context, key string) ([]byte, error) {
	svc := s3.New(s.Session)

	var err error
	var document *s3.GetObjectOutput
	var b []byte

	document, err = svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == s3.ErrCodeNoSuchKey {
				// specifically handle the "not found" case
				return nil, ErrNotFound
			}
		}

		return nil, err
	}

	b, err = ioutil.ReadAll(document.Body)
	if err != nil {
		return nil, err
	}

	return b, nil
}

// Remove removes the object stored at key, in the S3 Bucket.
func (s S3Storage) Remove(ctx context.Context, key string) error {
	svc := s3.New(s.Session)

	do := &s3.DeleteObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
	}

	if _, err := svc.DeleteObject(do); err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == s3.ErrCodeNoSuchKey {
				// specifically handle the "not found" case
				return ErrNotFound
			}
		}

		return ErrNotFound
	}

	return nil
}
